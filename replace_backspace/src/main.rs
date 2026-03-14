use std::fs;
use walkdir::WalkDir;

fn main() {
    let folder = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());

    for entry in WalkDir::new(&folder).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();

        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.contains(' ') {
                    let new_name = name.replace(' ', "_");
                    let new_path = path.with_file_name(&new_name);

                    match fs::rename(path, &new_path) {
                        Ok(_) => println!("Renamed: {} -> {}", name, new_name),
                        Err(e) => println!("Error renaming {}: {}", name, e),
                    }
                }
            }
        }
    }
}