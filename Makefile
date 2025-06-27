# MTGJSON Build Makefile
# Supports both Docker Bake builds and local Rust module building
# Cross-platform compatible (Linux, macOS, Windows)

.PHONY: help build build-local build-dev build-ci build-all clean test push
.PHONY: rust-local rust-wheel rust-debug rust-check troubleshoot install-rust-deps
.PHONY: rust-test rust-clean dev-cycle rust-dev-cycle full-dev-cycle setup-dev
.PHONY: platform-help clean-all run run-dev shell inspect benchmark logs
.PHONY: install-buildx setup-builder release

# Default registry and tag
REGISTRY ?= mtgjson
TAG ?= latest

# Platform detection
ifeq ($(OS),Windows_NT)
    PLATFORM := windows
    BUILD_SCRIPT := cmd /c build_rust.bat
    PYTHON := python
else
    UNAME_S := $(shell uname -s)
    ifeq ($(UNAME_S),Linux)
        PLATFORM := linux
    endif
    ifeq ($(UNAME_S),Darwin)
        PLATFORM := macos
    endif
    BUILD_SCRIPT := python ./build_rust.py
    PYTHON := python3
endif

# Colors for output (disable on Windows CMD)
ifeq ($(PLATFORM),windows)
    GREEN := 
    YELLOW := 
    RED := 
    NC := 
else
    GREEN := \033[0;32m
    YELLOW := \033[1;33m
    RED := \033[0;31m
    NC := \033[0m
endif

help: ## Show this help message
	@echo "$(GREEN)MTGJSON Build System ($(PLATFORM))$(NC)"
	@echo "================================="
	@echo ""
	@echo "$(YELLOW)Quick Start:$(NC)"
	@echo "  make rust-local     # Build Rust module locally (fastest)"
	@echo "  make build-local    # Build Docker image locally"
	@echo "  make build          # Production Docker build"
	@echo ""
	@echo "$(YELLOW)Local Rust Development:$(NC)"
	@echo "  make rust-local     # Build and install Rust module"
	@echo "  make rust-debug     # Build in debug mode (faster compile)"
	@echo "  make rust-wheel     # Build distributable wheel"
	@echo "  make rust-check     # Check Rust/maturin installation"
	@echo "  make troubleshoot   # Troubleshoot build issues"
	@echo ""
	@echo "$(YELLOW)Docker Commands:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(YELLOW)Platform Info:$(NC)"
	@echo "  Platform: $(PLATFORM)"
	@echo "  Build Script: $(BUILD_SCRIPT)"
	@echo "  Python: $(PYTHON)"
	@echo ""
	@echo "$(YELLOW)Environment Variables:$(NC)"
	@echo "  REGISTRY=$(REGISTRY)    # Docker registry"
	@echo "  TAG=$(TAG)         # Image tag"
	@echo ""
	@echo "$(YELLOW)Examples:$(NC)"
	@echo "  make rust-local              # Local Rust development"
	@echo "  make build-local && make run # Docker development"
	@echo "  REGISTRY=myregistry TAG=v1.0 make build"

build: ## Build production image (multi-platform, optimized caching)
	@echo "$(GREEN)Building production image with Docker Bake...$(NC)"
	docker buildx bake -f docker-bake.hcl mtgjson \
		--set="*.args.REGISTRY=$(REGISTRY)" \
		--set="*.args.TAG=$(TAG)"

build-local: ## Build for local development (single platform, faster)
	@echo "$(GREEN)Building local development image...$(NC)"
	docker buildx bake -f docker-bake.hcl local \
		--set="*.args.REGISTRY=$(REGISTRY)" \
		--set="*.args.TAG=$(TAG)" \
		--load

build-dev: ## Build development image with debug tools
	@echo "$(GREEN)Building development image with debug tools...$(NC)"
	docker buildx bake -f docker-bake.hcl mtgjson-dev \
		--set="*.args.REGISTRY=$(REGISTRY)" \
		--set="*.args.TAG=$(TAG)" \
		--load

build-ci: ## Build for CI/CD (pushes to registry)
	@echo "$(GREEN)Building CI image and pushing to registry...$(NC)"
	docker buildx bake -f docker-bake.hcl ci \
		--set="*.args.REGISTRY=$(REGISTRY)" \
		--set="*.args.TAG=$(TAG)"

build-all: ## Build all targets
	@echo "$(GREEN)Building all targets...$(NC)"
	docker buildx bake -f docker-bake.hcl all \
		--set="*.args.REGISTRY=$(REGISTRY)" \
		--set="*.args.TAG=$(TAG)"

rust-builder: ## Build only the Rust builder stage (for debugging)
	@echo "$(GREEN)Building Rust builder stage...$(NC)"
	docker buildx bake -f docker-bake.hcl rust-builder \
		--set="*.args.REGISTRY=$(REGISTRY)" \
		--set="*.args.TAG=$(TAG)"

test: ## Run tests in Docker
	@echo "$(GREEN)Running tests...$(NC)"
	docker buildx bake -f docker-bake.hcl test \
		--set="*.args.REGISTRY=$(REGISTRY)" \
		--set="*.args.TAG=$(TAG)"

# ================================
# Local Rust Module Building
# ================================

rust-local: ## Build and install Rust module locally (development mode)
	@echo "$(GREEN)Building Rust module locally...$(NC)"
	$(BUILD_SCRIPT)

rust-debug: ## Build Rust module in debug mode (faster compilation)
	@echo "$(GREEN)Building Rust module in debug mode...$(NC)"
ifeq ($(PLATFORM),windows)
	$(BUILD_SCRIPT) --debug
else
	$(BUILD_SCRIPT) --mode debug
endif

rust-wheel: ## Build distributable wheel
	@echo "$(GREEN)Building Rust wheel...$(NC)"
ifeq ($(PLATFORM),windows)
	$(BUILD_SCRIPT) --wheel
else
	$(BUILD_SCRIPT) --wheel
endif

rust-check: ## Check if Rust and maturin are properly installed
	@echo "$(GREEN)Checking Rust installation...$(NC)"
ifeq ($(PLATFORM),windows)
	$(BUILD_SCRIPT) --check
else
	$(BUILD_SCRIPT) --check-only
endif

troubleshoot: ## Print troubleshooting information
	@echo "$(GREEN)Gathering troubleshooting information...$(NC)"
ifeq ($(PLATFORM),windows)
	$(BUILD_SCRIPT) --troubleshoot
else
	$(BUILD_SCRIPT) --troubleshoot
endif

install-rust-deps: ## Install Rust and required dependencies
	@echo "$(GREEN)Installing Rust dependencies...$(NC)"
ifeq ($(PLATFORM),windows)
	@echo "Please install:"
	@echo "1. Rust: https://rustup.rs/"
	@echo "2. Visual Studio Build Tools: https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022"
	@echo "3. Python development headers (usually included with Python)"
else ifeq ($(PLATFORM),linux)
	@echo "Installing Rust and dependencies on Linux..."
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
	sudo apt update && sudo apt install -y python3-dev build-essential pkg-config libssl-dev
	$(PYTHON) -m pip install maturin
else ifeq ($(PLATFORM),macos)
	@echo "Installing Rust and dependencies on macOS..."
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
	xcode-select --install || true
	$(PYTHON) -m pip install maturin
endif

rust-test: ## Test the locally built Rust module
	@echo "$(GREEN)Testing Rust module...$(NC)"
	$(PYTHON) -c "import mtgjson_rust; print('✓ Module imported successfully')"
	$(PYTHON) -c "import mtgjson_rust; card = mtgjson_rust.MtgjsonCard(); print('✓ Card creation works')"
	$(PYTHON) -c "import mtgjson_rust; prices = mtgjson_rust.MtgjsonPrices(); print('✓ Prices creation works')"
	$(PYTHON) -c "import mtgjson_rust; proc = mtgjson_rust.ParallelProcessor(); print('✓ Parallel processor works')"
	@echo "$(GREEN)All Rust module tests passed!$(NC)"

rust-clean: ## Clean Rust build artifacts
	@echo "$(GREEN)Cleaning Rust build artifacts...$(NC)"
ifeq ($(PLATFORM),windows)
	if exist "mtgjson-rust\\target" rmdir /s /q "mtgjson-rust\\target"
	if exist "*.whl" del /f /q "*.whl"
else
	rm -rf mtgjson-rust/target/
	rm -f *.whl
endif

clean: ## Clean Docker build cache and Rust artifacts
	@echo "$(YELLOW)Cleaning build cache...$(NC)"
	@echo "Cleaning Docker build cache..."
	-docker buildx prune -f
	-docker system prune -f --volumes
	@echo "Cleaning Rust artifacts..."
	$(MAKE) rust-clean

clean-all: ## Clean everything including images and Rust artifacts
	@echo "$(RED)Cleaning all build resources...$(NC)"
	@echo "Cleaning Docker resources..."
	-docker buildx prune -af
	-docker system prune -af --volumes
	@echo "Cleaning Rust artifacts..."
	$(MAKE) rust-clean
	@echo "$(RED)All clean!$(NC)"

push: ## Push images to registry
	@echo "$(GREEN)Pushing images to $(REGISTRY)...$(NC)"
	docker push $(REGISTRY)/mtgjson:$(TAG)
	docker push $(REGISTRY)/rust-builder:$(TAG)

run: ## Run the built image locally
	@echo "$(GREEN)Running MTGJSON container...$(NC)"
	docker run --rm -it $(REGISTRY)/mtgjson:$(TAG)

run-dev: ## Run development image with shell
	@echo "$(GREEN)Running development container with shell...$(NC)"
	docker run --rm -it --entrypoint /bin/bash $(REGISTRY)/mtgjson:dev

shell: ## Get shell in running container
	@echo "$(GREEN)Getting shell in container...$(NC)"
	docker run --rm -it --entrypoint /bin/bash $(REGISTRY)/mtgjson:$(TAG)

inspect: ## Inspect the built image
	@echo "$(GREEN)Inspecting image...$(NC)"
	docker run --rm $(REGISTRY)/mtgjson:$(TAG) python3 -c "\
import mtgjson_rust; \
print('✓ Rust module loaded successfully'); \
print('Available classes:', [attr for attr in dir(mtgjson_rust) if not attr.startswith('_')])"

benchmark: ## Run a quick benchmark
	@echo "$(GREEN)Running benchmark...$(NC)"
	docker run --rm $(REGISTRY)/mtgjson:$(TAG) python3 -c "\
import time; \
import mtgjson_rust; \
start = time.time(); \
for i in range(1000): \
    card = mtgjson_rust.MtgjsonCard(); \
print(f'Created 1000 cards in {time.time() - start:.4f}s')"

logs: ## View build logs
	docker buildx bake -f docker-bake.hcl mtgjson --progress=plain

# Development helpers
install-buildx: ## Install Docker Buildx (if not available)
	@echo "$(GREEN)Installing Docker Buildx...$(NC)"
	docker buildx install

setup-builder: ## Setup multi-platform builder
	@echo "$(GREEN)Setting up multi-platform builder...$(NC)"
	docker buildx create --name mtgjson-builder --use --bootstrap
	docker buildx ls

# ================================
# Combined Workflows
# ================================

dev-cycle: build-local inspect ## Quick Docker development cycle: build and test
	@echo "$(GREEN)Docker development cycle complete!$(NC)"

rust-dev-cycle: rust-local rust-test ## Quick Rust development cycle: build and test locally
	@echo "$(GREEN)Rust development cycle complete!$(NC)"

full-dev-cycle: rust-local rust-test build-local inspect ## Full development cycle: Rust + Docker
	@echo "$(GREEN)Full development cycle complete!$(NC)"

setup-dev: install-rust-deps rust-check ## Setup development environment
	@echo "$(GREEN)Development environment setup complete!$(NC)"
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  make rust-local     # Build Rust module"
	@echo "  make rust-test      # Test Rust module"
	@echo "  make build-local    # Build Docker image"

# Production release cycle
release: clean build test push ## Full release cycle: clean, build, test, push
	@echo "$(GREEN)Release cycle complete!$(NC)"

# Platform-specific notes
platform-help: ## Show platform-specific help
	@echo "$(GREEN)Platform-Specific Information$(NC)"
	@echo "================================="
	@echo "Platform: $(PLATFORM)"
	@echo ""
ifeq ($(PLATFORM),windows)
	@echo "$(YELLOW)Windows Notes:$(NC)"
	@echo "- Use 'build_rust.bat' for Rust building"
	@echo "- Make sure Visual Studio Build Tools are installed"
	@echo "- Run as Administrator if you get permission errors"
	@echo "- Docker Desktop required for Docker commands"
	@echo ""
	@echo "$(YELLOW)Recommended Windows Setup:$(NC)"
	@echo "1. Install Rust: https://rustup.rs/"
	@echo "2. Install Visual Studio Build Tools"
	@echo "3. Run: make rust-check"
	@echo "4. Run: make rust-local"
else ifeq ($(PLATFORM),linux)
	@echo "$(YELLOW)Linux Notes:$(NC)"
	@echo "- Most dependencies available via package manager"
	@echo "- May need 'sudo' for system package installation"
	@echo "- Docker installation varies by distribution"
	@echo ""
	@echo "$(YELLOW)Quick Linux Setup:$(NC)"
	@echo "  make install-rust-deps  # Install everything needed"
	@echo "  make rust-local         # Build Rust module"
else ifeq ($(PLATFORM),macos)
	@echo "$(YELLOW)macOS Notes:$(NC)"
	@echo "- Xcode Command Line Tools required"
	@echo "- Homebrew recommended for dependencies"
	@echo "- Docker Desktop available from Docker website"
	@echo ""
	@echo "$(YELLOW)Quick macOS Setup:$(NC)"
	@echo "  make install-rust-deps  # Install everything needed"
	@echo "  make rust-local         # Build Rust module"
endif