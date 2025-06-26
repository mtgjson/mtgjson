# MTGJSON Rust Progress

## **Current Issues (75 remaining errors)**

### 1. **Documentation Comments** (Multiple Files)
- **Files**: `game_formats.rs`, `identifiers.rs`, `leadership_skills.rs`
- **Issue**: Inner doc comments (`//!`) in wrong locations
- **Priority**: Medium - Easy to fix

### 2. **Type Conversion Issues** 
- **Files**: `foreign_data.rs`, `sealed_product.rs`, `prices.rs`
- **Issue**: `serde_json::Value` cannot convert to `PyObject` with `into_py()`
- **Priority**: High - Need alternative conversion approach

### 3. **Method Parameter Types**
- **Files**: Various (PyO3 methods)
- **Issue**: Function parameters need PyO3-compatible types
- **Priority**: High - Core functionality

### 4. **Unused Variables/Imports**
- **Files**: Multiple
- **Issue**: Cleanup warnings from removed serde functionality  
- **Priority**: Low - Cosmetic