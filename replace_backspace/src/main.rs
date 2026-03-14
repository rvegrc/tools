use std::borrow::Cow;
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

#[inline]
fn is_word_char(c: char) -> bool {
    c.is_alphanumeric()
}

#[inline]
fn is_two_word_special_case(s: &str) -> bool {
    let mut parts = s.split(' ');

    let a = match parts.next() {
        Some(v) if !v.is_empty() => v,
        _ => return false,
    };

    let b = match parts.next() {
        Some(v) if !v.is_empty() => v,
        _ => return false,
    };

    if parts.next().is_some() {
        return false;
    }

    a.chars().all(is_word_char) && b.chars().all(is_word_char)
}

fn normalize_stem(stem: &str) -> Cow<'_, str> {
    if !stem.contains([' ', '-', '—']) {
        return Cow::Borrowed(stem);
    }

    let mut out = String::with_capacity(stem.len());
    let mut chars = stem.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            ' ' => {
                // Consume consecutive spaces
                while matches!(chars.peek(), Some(' ')) {
                    chars.next();
                }

                match chars.peek().copied() {
                    Some('-' | '—') => {
                        // Skip spaces before dash
                    }
                    Some(_) => {
                        // Keep a single space for now; final conversion happens later
                        if !out.is_empty() && !out.ends_with(' ') {
                            out.push(' ');
                        }
                    }
                    None => {
                        // Ignore trailing spaces
                    }
                }
            }

            '-' | '—' => {
                // Remove space before dash
                if out.ends_with(' ') {
                    out.pop();
                }

                out.push('-');

                // Skip spaces after dash
                while matches!(chars.peek(), Some(' ')) {
                    chars.next();
                }
            }

            _ => out.push(c),
        }
    }

    if out.is_empty() {
        return Cow::Owned(out);
    }

    if is_two_word_special_case(&out) {
        let mut final_name = String::with_capacity(out.len());
        let mut replaced = false;

        for ch in out.chars() {
            if ch == ' ' && !replaced {
                final_name.push('-');
                replaced = true;
            } else {
                final_name.push(ch);
            }
        }

        Cow::Owned(final_name)
    } else if out.contains(' ') {
        let mut final_name = String::with_capacity(out.len());

        for ch in out.chars() {
            if ch == ' ' {
                final_name.push('_');
            } else {
                final_name.push(ch);
            }
        }

        Cow::Owned(final_name)
    } else {
        Cow::Owned(out)
    }
}

fn normalize_filename(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    let ext = path.extension().and_then(|e| e.to_str());

    let new_stem = normalize_stem(stem);

    Some(match ext {
        Some(ext) if !ext.is_empty() => {
            let mut s = String::with_capacity(new_stem.len() + 1 + ext.len());
            s.push_str(&new_stem);
            s.push('.');
            s.push_str(ext);
            s
        }
        _ => new_stem.into_owned(),
    })
}

fn main() {
    let folder = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".".to_string());

    for entry in WalkDir::new(&folder).into_iter().filter_map(Result::ok) {
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let Some(old_name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };

        let Some(new_name) = normalize_filename(path) else {
            continue;
        };

        if old_name == new_name {
            continue;
        }

        let new_path = path.with_file_name(&new_name);

        match fs::rename(path, &new_path) {
            Ok(_) => println!("Renamed: {} -> {}", old_name, new_name),
            Err(e) => eprintln!("Error renaming {}: {}", old_name, e),
        }
    }
}