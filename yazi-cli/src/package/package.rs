use std::{collections::HashMap, path::Path, str::FromStr};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use yazi_fs::{Xdg, provider::{Provider, local::Local}};
use yazi_macro::{ok_or_not_found, outln};

use super::Dependency;

#[derive(Default)]
pub(crate) struct Package {
	pub(crate) plugins: Vec<Dependency>,
	pub(crate) flavors: Vec<Dependency>,
}

impl Package {
	pub(crate) async fn load() -> Result<Self> {
		Self::load_from(&Xdg::config_dir()).await
	}

	pub(crate) async fn add_many(&mut self, uses: &[String]) -> Result<()> {
		for u in uses {
			let r = self.add(u).await;
			self.save().await?;
			r?;
		}
		Ok(())
	}

	pub(crate) async fn delete_many(&mut self, uses: &[String]) -> Result<()> {
		for u in uses {
			let r = self.delete(u).await;
			self.save().await?;
			r?;
		}
		Ok(())
	}

	pub(crate) async fn install(&mut self) -> Result<()> {
		macro_rules! go {
			($dep:expr) => {
				let r = $dep.install().await;
				self.save().await?;
				r?;
			};
		}

		for i in 0..self.plugins.len() {
			go!(self.plugins[i]);
		}
		for i in 0..self.flavors.len() {
			go!(self.flavors[i]);
		}
		Ok(())
	}

	pub(crate) async fn upgrade_many(&mut self, uses: &[String]) -> Result<()> {
		macro_rules! go {
			($dep:expr) => {
				if uses.is_empty() || uses.contains(&$dep.r#use) {
					let r = $dep.upgrade().await;
					self.save().await?;
					r?;
				}
			};
		}

		for i in 0..self.plugins.len() {
			go!(self.plugins[i]);
		}
		for i in 0..self.flavors.len() {
			go!(self.flavors[i]);
		}
		Ok(())
	}

	pub(crate) fn print(&self) -> Result<()> {
		outln!("Plugins:")?;
		for d in &self.plugins {
			if d.rev.is_empty() {
				outln!("\t{}", d.r#use)?;
			} else {
				outln!("\t{} ({})", d.r#use, d.rev)?;
			}
		}

		outln!("Flavors:")?;
		for d in &self.flavors {
			if d.rev.is_empty() {
				outln!("\t{}", d.r#use)?;
			} else {
				outln!("\t{} ({})", d.r#use, d.rev)?;
			}
		}

		Ok(())
	}

	async fn add(&mut self, r#use: &str) -> Result<()> {
		let mut dep = Dependency::from_str(r#use)?;
		if let Some(d) = self.identical(&dep) {
			bail!(
				"{} `{}` already exists in package.toml",
				if d.is_flavor { "Flavor" } else { "Plugin" },
				dep.name
			)
		}

		dep.add().await?;
		if dep.is_flavor {
			self.flavors.push(dep);
		} else {
			self.plugins.push(dep);
		}
		Ok(())
	}

	async fn delete(&mut self, r#use: &str) -> Result<()> {
		let Some(dep) = self.identical(&Dependency::from_str(r#use)?).cloned() else {
			bail!("`{}` was not found in package.toml", r#use)
		};

		dep.delete().await?;
		if dep.is_flavor {
			self.flavors.retain(|d| !d.identical(&dep));
		} else {
			self.plugins.retain(|d| !d.identical(&dep));
		}
		Ok(())
	}

	async fn save(&self) -> Result<()> {
		self.save_to(&Xdg::config_dir()).await
	}

	async fn load_from(config_dir: &Path) -> Result<Self> {
		#[derive(Default, Deserialize)]
		struct Outer {
			#[serde(default)]
			plugin: Shadow,
			#[serde(default)]
			flavor: Shadow,
		}
		#[derive(Default, Deserialize)]
		struct Shadow {
			#[serde(default)]
			deps: Vec<UseEntry>,
		}
		#[derive(Deserialize)]
		struct UseEntry {
			r#use: String,
		}

		let s = ok_or_not_found!(
			Local::regular(&config_dir.join("package.toml")).read_to_string().await
		);
		let outer = toml::from_str::<Outer>(&s)?;

		let plugins = outer
			.plugin
			.deps
			.into_iter()
			.map(|e| Dependency::from_str(&e.r#use))
			.collect::<Result<Vec<_>>>()?;
		let mut flavors = outer
			.flavor
			.deps
			.into_iter()
			.map(|e| Dependency::from_str(&e.r#use))
			.collect::<Result<Vec<_>>>()?;
		flavors.iter_mut().for_each(|d| d.is_flavor = true);

		let plugin_deps =
			Self::load_dep_toml(&config_dir.join("plugins").join("dependency.toml"), "plugin")
				.await;
		let flavor_deps =
			Self::load_dep_toml(&config_dir.join("flavors").join("dependency.toml"), "flavor")
				.await;

		let mut pkg = Self { plugins, flavors };
		for dep in &mut pkg.plugins {
			if let Some((rev, hash)) = plugin_deps.get(&dep.r#use) {
				dep.rev = rev.clone();
				dep.hash = hash.clone();
			}
		}
		for dep in &mut pkg.flavors {
			if let Some((rev, hash)) = flavor_deps.get(&dep.r#use) {
				dep.rev = rev.clone();
				dep.hash = hash.clone();
			}
		}

		Ok(pkg)
	}

	async fn save_to(&self, config_dir: &Path) -> Result<()> {
		self.save_package_toml(config_dir).await?;
		Self::save_dep_toml(
			&self.plugins,
			"plugin",
			&config_dir.join("plugins").join("dependency.toml"),
		)
		.await?;
		Self::save_dep_toml(
			&self.flavors,
			"flavor",
			&config_dir.join("flavors").join("dependency.toml"),
		)
		.await
	}

	async fn save_package_toml(&self, config_dir: &Path) -> Result<()> {
		#[derive(Serialize)]
		struct Outer<'a> {
			plugin: Shadow<'a>,
			flavor: Shadow<'a>,
		}
		#[derive(Serialize)]
		struct Shadow<'a> {
			deps: Vec<UseEntry<'a>>,
		}
		#[derive(Serialize)]
		struct UseEntry<'a> {
			r#use: &'a str,
		}

		let out = Outer {
			plugin: Shadow {
				deps: self.plugins.iter().map(|d| UseEntry { r#use: &d.r#use }).collect(),
			},
			flavor: Shadow {
				deps: self.flavors.iter().map(|d| UseEntry { r#use: &d.r#use }).collect(),
			},
		};
		let s = toml::to_string_pretty(&out)?;
		Local::regular(&config_dir.join("package.toml"))
			.write(s)
			.await
			.context("Failed to write package.toml")
	}

	async fn save_dep_toml(deps: &[Dependency], section: &str, path: &Path) -> Result<()> {
		let mut deps_map = toml::Table::new();
		for dep in deps {
			if !dep.rev.is_empty() || !dep.hash.is_empty() {
				let mut entry = toml::Table::new();
				entry.insert("rev".into(), toml::Value::String(dep.rev.clone()));
				entry.insert("hash".into(), toml::Value::String(dep.hash.clone()));
				deps_map.insert(dep.r#use.clone(), toml::Value::Table(entry));
			}
		}

		let mut section_table = toml::Table::new();
		section_table.insert("deps".into(), toml::Value::Table(deps_map));

		let mut table = toml::Table::new();
		table.insert(section.into(), toml::Value::Table(section_table));

		if let Some(parent) = path.parent() {
			tokio::fs::create_dir_all(parent).await?;
		}
		let s = toml::to_string_pretty(&table)?;
		Local::regular(path)
			.write(s)
			.await
			.with_context(|| format!("Failed to write {}", path.display()))
	}

	async fn load_dep_toml(path: &Path, section: &str) -> HashMap<String, (String, String)> {
		let Ok(s) = Local::regular(path).read_to_string().await else {
			return HashMap::new();
		};
		let Ok(table) = toml::from_str::<toml::Table>(&s) else {
			return HashMap::new();
		};
		let Some(deps) = table
			.get(section)
			.and_then(|s| s.get("deps"))
			.and_then(|d| d.as_table())
		else {
			return HashMap::new();
		};
		deps.iter()
			.filter_map(|(k, v)| {
				let t = v.as_table()?;
				let rev = t.get("rev")?.as_str()?.to_owned();
				let hash = t.get("hash")?.as_str()?.to_owned();
				Some((k.clone(), (rev, hash)))
			})
			.collect()
	}

	fn identical(&self, other: &Dependency) -> Option<&Dependency> {
		self.plugins.iter().chain(&self.flavors).find(|d| d.identical(other))
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use super::*;

	fn make_dep(use_str: &str, rev: &str, hash: &str) -> Dependency {
		let mut dep = Dependency::from_str(use_str).unwrap();
		dep.rev = rev.to_owned();
		dep.hash = hash.to_owned();
		dep
	}

	// Test 1: New plugin — package.toml has only `use`, plugins/dependency.toml has rev+hash
	#[tokio::test]
	async fn test_new_plugin_creates_split_files() {
		let dir = tempfile::tempdir().unwrap();

		let pkg = Package {
			plugins: vec![make_dep("owner/repo:plugin", "abc123", "def456")],
			flavors: vec![],
		};
		pkg.save_to(dir.path()).await.unwrap();

		let pkg_toml = std::fs::read_to_string(dir.path().join("package.toml")).unwrap();
		assert!(pkg_toml.contains("owner/repo:plugin"), "use must appear in package.toml");
		assert!(!pkg_toml.contains("abc123"), "rev must NOT appear in package.toml");
		assert!(!pkg_toml.contains("def456"), "hash must NOT appear in package.toml");

		let dep_toml =
			std::fs::read_to_string(dir.path().join("plugins").join("dependency.toml")).unwrap();
		assert!(dep_toml.contains("abc123"), "rev must appear in plugins/dependency.toml");
		assert!(dep_toml.contains("def456"), "hash must appear in plugins/dependency.toml");
		assert!(dep_toml.contains("[plugin"), "section must be [plugin.deps.*]");
	}

	// Test 2: Plugin removed — entry gone from both files; dep.toml exists but is empty
	#[tokio::test]
	async fn test_delete_plugin_removes_from_both_files() {
		let dir = tempfile::tempdir().unwrap();

		let mut pkg = Package {
			plugins: vec![make_dep("owner/repo:plugin", "abc123", "def456")],
			flavors: vec![],
		};
		pkg.save_to(dir.path()).await.unwrap();

		// Remove the plugin
		pkg.plugins.clear();
		pkg.save_to(dir.path()).await.unwrap();

		let loaded = Package::load_from(dir.path()).await.unwrap();
		assert!(loaded.plugins.is_empty(), "plugins must be empty after removal");

		let dep_toml =
			std::fs::read_to_string(dir.path().join("plugins").join("dependency.toml")).unwrap();
		assert!(!dep_toml.contains("owner/repo:plugin"), "removed entry must not be in dep.toml");
		assert!(!dep_toml.contains("abc123"), "removed rev must not be in dep.toml");
	}

	// Test 3a: Old-format package.toml — rev/hash silently ignored on load
	#[tokio::test]
	async fn test_migrate_old_format_drops_rev_hash() {
		let dir = tempfile::tempdir().unwrap();
		std::fs::write(
			dir.path().join("package.toml"),
			r#"
[plugin]
deps = [{ use = "owner/repo:plugin", rev = "abc123", hash = "def456" }]

[flavor]
deps = []
"#,
		)
		.unwrap();

		let pkg = Package::load_from(dir.path()).await.unwrap();
		assert_eq!(pkg.plugins.len(), 1);
		assert_eq!(pkg.plugins[0].r#use, "owner/repo:plugin");
		assert_eq!(pkg.plugins[0].rev, "", "rev must be empty — no dependency.toml exists");
		assert_eq!(pkg.plugins[0].hash, "", "hash must be empty — no dependency.toml exists");
	}

	// Test 3b: After migration, save() produces new split format
	#[tokio::test]
	async fn test_migrate_and_save_strips_rev_hash_from_package_toml() {
		let dir = tempfile::tempdir().unwrap();
		std::fs::write(
			dir.path().join("package.toml"),
			r#"
[plugin]
deps = [{ use = "owner/repo:plugin", rev = "abc123", hash = "def456" }]
[flavor]
deps = []
"#,
		)
		.unwrap();

		let pkg = Package::load_from(dir.path()).await.unwrap();
		pkg.save_to(dir.path()).await.unwrap();

		let pkg_toml = std::fs::read_to_string(dir.path().join("package.toml")).unwrap();
		assert!(!pkg_toml.contains("abc123"), "rev must NOT appear after save");
		assert!(!pkg_toml.contains("def456"), "hash must NOT appear after save");
		assert!(pkg_toml.contains("owner/repo:plugin"), "use must still be in package.toml");
	}

	// Test 4: Full load/save roundtrip preserves all fields
	#[tokio::test]
	async fn test_roundtrip_preserves_all_fields() {
		let dir = tempfile::tempdir().unwrap();

		let original = Package {
			plugins: vec![
				make_dep("owner/repo:plugin-a", "rev1", "hash1"),
				make_dep("owner/repo:plugin-b", "rev2", "hash2"),
			],
			flavors: vec![make_dep("owner/repo:my-flavor", "rev3", "hash3")],
		};
		original.save_to(dir.path()).await.unwrap();

		let loaded = Package::load_from(dir.path()).await.unwrap();

		assert_eq!(loaded.plugins.len(), 2);
		assert_eq!(loaded.plugins[0].r#use, "owner/repo:plugin-a");
		assert_eq!(loaded.plugins[0].rev, "rev1");
		assert_eq!(loaded.plugins[0].hash, "hash1");
		assert_eq!(loaded.plugins[1].r#use, "owner/repo:plugin-b");
		assert_eq!(loaded.plugins[1].rev, "rev2");
		assert_eq!(loaded.plugins[1].hash, "hash2");

		assert_eq!(loaded.flavors.len(), 1);
		assert_eq!(loaded.flavors[0].r#use, "owner/repo:my-flavor");
		assert_eq!(loaded.flavors[0].rev, "rev3");
		assert_eq!(loaded.flavors[0].hash, "hash3");
		assert!(loaded.flavors[0].is_flavor, "is_flavor must be set on loaded flavors");
	}

	// Test 5: Missing dependency.toml — load succeeds, deps have empty rev/hash
	#[tokio::test]
	async fn test_missing_dep_toml_loads_with_empty_rev_hash() {
		let dir = tempfile::tempdir().unwrap();
		std::fs::write(
			dir.path().join("package.toml"),
			r#"
[plugin]
deps = [{ use = "owner/repo:plugin" }]
[flavor]
deps = []
"#,
		)
		.unwrap();

		let pkg = Package::load_from(dir.path()).await.unwrap();
		assert_eq!(pkg.plugins.len(), 1);
		assert_eq!(pkg.plugins[0].rev, "");
		assert_eq!(pkg.plugins[0].hash, "");
	}

	// Test 6a: Flavor install — written to flavors/dependency.toml, not plugins/
	#[tokio::test]
	async fn test_new_flavor_creates_flavor_dep_toml() {
		let dir = tempfile::tempdir().unwrap();

		let pkg = Package {
			plugins: vec![],
			flavors: vec![make_dep("owner/repo:my-flavor", "frev", "fhash")],
		};
		pkg.save_to(dir.path()).await.unwrap();

		let pkg_toml = std::fs::read_to_string(dir.path().join("package.toml")).unwrap();
		assert!(pkg_toml.contains("owner/repo:my-flavor"));
		assert!(!pkg_toml.contains("frev"), "rev must NOT be in package.toml");

		let dep_toml =
			std::fs::read_to_string(dir.path().join("flavors").join("dependency.toml")).unwrap();
		assert!(dep_toml.contains("frev"), "rev must be in flavors/dependency.toml");
		assert!(dep_toml.contains("fhash"), "hash must be in flavors/dependency.toml");
		assert!(dep_toml.contains("[flavor"), "section must be [flavor.deps.*]");
		assert!(!dep_toml.contains("[plugin"), "[plugin] must NOT appear in flavors dep.toml");

		// plugins/dependency.toml should have no flavor entries
		let plugin_dep_path = dir.path().join("plugins").join("dependency.toml");
		if plugin_dep_path.exists() {
			let plugin_dep_toml = std::fs::read_to_string(&plugin_dep_path).unwrap();
			assert!(!plugin_dep_toml.contains("frev"));
		}
	}

	// Test 6b: Flavor removal — entry gone from flavors/dependency.toml
	#[tokio::test]
	async fn test_delete_flavor_removes_from_dep_toml() {
		let dir = tempfile::tempdir().unwrap();

		let mut pkg = Package {
			plugins: vec![],
			flavors: vec![make_dep("owner/repo:my-flavor", "frev", "fhash")],
		};
		pkg.save_to(dir.path()).await.unwrap();

		pkg.flavors.clear();
		pkg.save_to(dir.path()).await.unwrap();

		let dep_toml =
			std::fs::read_to_string(dir.path().join("flavors").join("dependency.toml")).unwrap();
		assert!(!dep_toml.contains("frev"), "removed rev must not be in dep.toml");
		assert!(!dep_toml.contains("my-flavor"), "removed use must not be in dep.toml");
	}

	// Test 7: Pinned version (=rev prefix) preserved across save/load
	#[tokio::test]
	async fn test_pinned_version_roundtrip() {
		let dir = tempfile::tempdir().unwrap();

		let pkg = Package {
			plugins: vec![make_dep("owner/repo:plugin", "=pinned123", "hash1")],
			flavors: vec![],
		};
		pkg.save_to(dir.path()).await.unwrap();

		let loaded = Package::load_from(dir.path()).await.unwrap();
		assert_eq!(
			loaded.plugins[0].rev, "=pinned123",
			"pinned rev (= prefix) must survive save/load"
		);
	}

	// Test 8a: Old package.toml format — unknown fields (rev, hash) are silently ignored
	#[test]
	fn test_parse_package_toml_ignores_unknown_fields() {
		#[derive(serde::Deserialize, Default)]
		struct Outer {
			#[serde(default)]
			plugin: Shadow,
		}
		#[derive(serde::Deserialize, Default)]
		struct Shadow {
			#[serde(default)]
			deps: Vec<UseEntry>,
		}
		#[derive(serde::Deserialize)]
		struct UseEntry {
			r#use: String,
		}

		let s = r#"
[plugin]
deps = [{ use = "owner/repo:p", rev = "old_rev", hash = "old_hash" }]
"#;
		let outer = toml::from_str::<Outer>(s).unwrap();
		assert_eq!(outer.plugin.deps.len(), 1);
		assert_eq!(outer.plugin.deps[0].r#use, "owner/repo:p");
		// Struct has no `rev` or `hash` fields — extra TOML keys are silently ignored
	}

	// Test 8b: dep.toml Table round-trip via save_dep_toml / load_dep_toml
	#[tokio::test]
	async fn test_dep_toml_table_roundtrip() {
		let dir = tempfile::tempdir().unwrap();
		let deps = vec![
			make_dep("owner/repo:plugin-a", "rev1", "hash1"),
			make_dep("owner/repo:plugin-b", "rev2", "hash2"),
		];
		let path = dir.path().join("dep.toml");

		Package::save_dep_toml(&deps, "plugin", &path).await.unwrap();
		let loaded = Package::load_dep_toml(&path, "plugin").await;

		assert_eq!(
			loaded.get("owner/repo:plugin-a"),
			Some(&("rev1".to_owned(), "hash1".to_owned()))
		);
		assert_eq!(
			loaded.get("owner/repo:plugin-b"),
			Some(&("rev2".to_owned(), "hash2".to_owned()))
		);
		assert_eq!(loaded.len(), 2);
	}

	// Test 8c: Empty Package saves and reloads without panics
	#[tokio::test]
	async fn test_empty_package_save_load() {
		let dir = tempfile::tempdir().unwrap();

		let pkg = Package::default();
		pkg.save_to(dir.path()).await.unwrap();

		let loaded = Package::load_from(dir.path()).await.unwrap();
		assert!(loaded.plugins.is_empty());
		assert!(loaded.flavors.is_empty());

		// Files must be valid TOML (readable without error)
		let pkg_toml = std::fs::read_to_string(dir.path().join("package.toml")).unwrap();
		toml::from_str::<toml::Table>(&pkg_toml).expect("package.toml must be valid TOML");
	}
}
