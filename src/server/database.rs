use omnipaxos_kv::common::kv::KVCommand;
use std::collections::HashMap;

pub struct Database {
    db: HashMap<String, String>,
}

impl Database {
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }

    pub fn handle_command(&mut self, command: KVCommand) -> Option<Option<String>> {
        match command {
            KVCommand::Put(key, value) => {
                self.db.insert(key, value);
                None
            }
            KVCommand::Delete(key) => {
                self.db.remove(&key);
                None
            }
            KVCommand::Get(key) => Some(self.db.get(&key).map(|v| v.clone())),
        }
    }

    pub fn state(&self) -> &HashMap<String, String> {
        &self.db
    }

    pub fn read_only_result(&self, command: &KVCommand) -> Option<Option<String>> {
        match command {
            KVCommand::Put(_, _) | KVCommand::Delete(_) => None,
            KVCommand::Get(key) => Some(self.db.get(key).cloned()),
        }
    }
}
