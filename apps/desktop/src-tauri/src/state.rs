use std::path::PathBuf;
use std::sync::RwLock;

#[derive(Default)]
pub struct AppState {
    database_path: RwLock<Option<PathBuf>>,
}

impl AppState {
    pub fn database_path(&self) -> Option<PathBuf> {
        self.database_path
            .read()
            .expect("database_path lock poisoned")
            .clone()
    }

    pub fn set_database_path(&self, path: PathBuf) {
        *self
            .database_path
            .write()
            .expect("database_path lock poisoned") = Some(path);
    }
}
