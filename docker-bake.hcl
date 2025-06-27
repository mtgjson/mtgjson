// Docker Bake configuration for optimized MTGJSON builds
variable "RUST_VERSION" {
  default = "1.82"
}

variable "PYTHON_VERSION" {
  default = "3.11"
}

variable "MATURIN_VERSION" {
  default = "1.9.0"
}

variable "REGISTRY" {
  default = "mtgjson"
}

variable "TAG" {
  default = "latest"
}

// Build contexts for better caching
variable "CACHE_FROM" {
  default = [
    "type=gha",
    "type=registry,ref=${REGISTRY}/rust-builder:cache",
    "type=registry,ref=${REGISTRY}/mtgjson:cache"
  ]
}

variable "CACHE_TO" {
  default = [
    "type=gha,mode=max",
    "type=registry,ref=${REGISTRY}/rust-builder:cache,mode=max",
    "type=registry,ref=${REGISTRY}/mtgjson:cache,mode=max"
  ]
}

// Group for building all targets
group "default" {
  targets = ["mtgjson"]
}

group "all" {
  targets = ["rust-builder", "mtgjson", "mtgjson-dev"]
}

// Rust builder stage - optimized for caching
target "rust-builder" {
  dockerfile = "Dockerfile"
  target = "rust-builder"
  context = "."
  
  args = {
    RUST_VERSION = RUST_VERSION
    PYTHON_VERSION = PYTHON_VERSION
    MATURIN_VERSION = MATURIN_VERSION
    BUILD_MODE = "release"
  }
  
  cache-from = [
    "type=gha,scope=rust-builder",
    "type=registry,ref=${REGISTRY}/rust-builder:cache"
  ]
  
  cache-to = [
    "type=gha,scope=rust-builder,mode=max",
    "type=registry,ref=${REGISTRY}/rust-builder:cache,mode=max"
  ]
  
  platforms = ["linux/amd64", "linux/arm64"]
  
  tags = [
    "${REGISTRY}/rust-builder:${TAG}",
    "${REGISTRY}/rust-builder:cache"
  ]
}

// Main application target
target "mtgjson" {
  dockerfile = "Dockerfile"
  context = "."
  target = "final"
  
  args = {
    RUST_VERSION = RUST_VERSION
    PYTHON_VERSION = PYTHON_VERSION
    MATURIN_VERSION = MATURIN_VERSION
    BUILD_MODE = "release"
    INSTALL_DEV_TOOLS = "false"
  }
  
  cache-from = [
    "type=gha,scope=mtgjson",
    "type=gha,scope=rust-builder",
    "type=registry,ref=${REGISTRY}/mtgjson:cache",
    "type=registry,ref=${REGISTRY}/rust-builder:cache"
  ]
  
  cache-to = [
    "type=gha,scope=mtgjson,mode=max",
    "type=registry,ref=${REGISTRY}/mtgjson:cache,mode=max"
  ]
  
  platforms = ["linux/amd64", "linux/arm64"]
  
  tags = [
    "${REGISTRY}/mtgjson:${TAG}",
    "${REGISTRY}/mtgjson:cache"
  ]
  
  output = ["type=docker"]
}

// Development target with debug symbols and dev tools
target "mtgjson-dev" {
  inherits = ["mtgjson"]
  
  args = {
    RUST_VERSION = RUST_VERSION
    PYTHON_VERSION = PYTHON_VERSION
    MATURIN_VERSION = MATURIN_VERSION
    BUILD_MODE = "debug"
    INSTALL_DEV_TOOLS = "true"
  }
  
  tags = [
    "${REGISTRY}/mtgjson:dev",
    "${REGISTRY}/mtgjson:debug"
  ]
  
  cache-from = [
    "type=gha,scope=mtgjson-dev",
    "type=gha,scope=mtgjson",
    "type=gha,scope=rust-builder"
  ]
  
  cache-to = [
    "type=gha,scope=mtgjson-dev,mode=max"
  ]
}

// Local development target (no multi-platform)
target "local" {
  inherits = ["mtgjson"]
  platforms = []
  
  args = {
    RUST_VERSION = RUST_VERSION
    PYTHON_VERSION = PYTHON_VERSION
    MATURIN_VERSION = MATURIN_VERSION
    BUILD_MODE = "release"
    INSTALL_DEV_TOOLS = "false"
  }
  
  cache-from = [
    "type=gha,scope=local"
  ]
  
  cache-to = [
    "type=gha,scope=local,mode=max"
  ]
  
  tags = [
    "${REGISTRY}/mtgjson:local"
  ]
}

// CI/CD optimized target
target "ci" {
  inherits = ["mtgjson"]
  
  args = {
    RUST_VERSION = RUST_VERSION
    PYTHON_VERSION = PYTHON_VERSION
    MATURIN_VERSION = MATURIN_VERSION
    BUILD_MODE = "release"
    INSTALL_DEV_TOOLS = "false"
  }
  
  cache-from = [
    "type=gha,scope=ci",
    "type=gha,scope=rust-builder",
    "type=registry,ref=${REGISTRY}/mtgjson:cache",
    "type=registry,ref=${REGISTRY}/rust-builder:cache"
  ]
  
  cache-to = [
    "type=gha,scope=ci,mode=max",
    "type=registry,ref=${REGISTRY}/mtgjson:cache,mode=max",
    "type=registry,ref=${REGISTRY}/rust-builder:cache,mode=max"
  ]
  
  output = ["type=registry"]
}

// Testing target
target "test" {
  dockerfile = "Dockerfile"
  target = "test"
  context = "."
  
  args = {
    RUST_VERSION = RUST_VERSION
    PYTHON_VERSION = PYTHON_VERSION
    MATURIN_VERSION = MATURIN_VERSION
    BUILD_MODE = "release"
  }
  
  cache-from = [
    "type=gha,scope=test"
  ]
  
  cache-to = [
    "type=gha,scope=test,mode=max"
  ]
}