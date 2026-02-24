# KeyName Validation: Path Traversal Prevention Sound

- **Severity**: Informational (green)
- **Category**: Input Validation
- **Status**: Verified clean

## Summary

The `KeyName` type provides validated key names that prevent path traversal attacks. The validation rejects empty names, path separators (`/`, `\`, `\0`), and `.`/`..`. All three file-based backends (FileKeyring, SystemdCredsKeyring) call `KeyName::validate()` before constructing file paths. The validation is thoroughly tested including adversarial inputs.

## Verified Properties

1. **Rejects path separators**: `../escape`, `sub/dir`, `back\slash`, `null\0byte` — all rejected
2. **Rejects dot names**: `.` and `..` — rejected
3. **Rejects empty**: `""` — rejected
4. **Allows legitimate names**: `peer-key`, `with.dots`, `MixedCase`, `123numeric` — accepted
5. **Used consistently**: Both `file.rs:62` and `systemd_creds.rs:43` call `KeyName::validate()` before path construction

## Test Coverage

- `test_key_name_valid` — 6 valid names
- `test_key_name_empty` — empty rejection
- `test_key_name_path_separators` — 4 adversarial paths
- `test_key_name_dot_names` — `.` and `..`
- Integration test `test_file_keyring_path_traversal_prevention` — 7 adversarial names against live keyring
- `test_key_handle_invalid_key_name` — `../escape` rejected at KeyHandle construction

## No Issues Found

The validation is thorough and consistently applied across all backends.
