"""
Build script for the mtgjson-rust Python extension module.
"""

import os
import subprocess
import sys
import shutil
from pathlib import Path

def check_rust_installed():
    """Check if Rust is installed on the system."""
    try:
        result = subprocess.run(['rustc', '--version'], capture_output=True, text=True)
        print(f"✓ Rust found: {result.stdout.strip()}")
        return True
    except FileNotFoundError:
        print("✗ Rust not found. Please install Rust from https://rustup.rs/")
        return False

def check_maturin_installed():
    """Check if maturin is installed and install it if not."""
    try:
        result = subprocess.run(['maturin', '--version'], capture_output=True, text=True)
        print(f"✓ Maturin found: {result.stdout.strip()}")
        return True
    except FileNotFoundError:
        print("✗ Maturin not found. Installing...")
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install', 'maturin'], check=True)
            print("✓ Maturin installed successfully")
            return True
        except subprocess.CalledProcessError:
            print("✗ Failed to install maturin")
            return False

def build_rust_module(mode='release'):
    """Build the Rust module using maturin."""
    rust_dir = Path('mtgjson-rust')
    
    if not rust_dir.exists():
        print(f"✗ Rust directory {rust_dir} not found")
        return False
    
    print(f"Building Rust module in {mode} mode...")
    
    original_dir = os.getcwd()
    os.chdir(rust_dir)
    
    try:
        # Build command
        cmd = ['maturin', 'develop']
        if mode == 'release':
            cmd.append('--release')
        
        result = subprocess.run(cmd, check=True)
        print("✓ Rust module built and installed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to build Rust module: {e}")
        return False
    finally:
        os.chdir(original_dir)

def build_wheel():
    """Build a wheel for distribution."""
    rust_dir = Path('mtgjson-rust')
    
    if not rust_dir.exists():
        print(f"✗ Rust directory {rust_dir} not found")
        return False
    
    print("Building wheel...")
    
    # Change to the Rust directory
    original_dir = os.getcwd()
    os.chdir(rust_dir)
    
    try:
        # Build wheel
        result = subprocess.run(['maturin', 'build', '--release'], check=True)
        print("✓ Wheel built successfully")
        
        # Find and copy the wheel to the project root
        target_dir = Path('target/wheels')
        if target_dir.exists():
            wheels = list(target_dir.glob('*.whl'))
            if wheels:
                latest_wheel = max(wheels, key=lambda p: p.stat().st_mtime)
                shutil.copy(latest_wheel, '../')
                print(f"✓ Wheel copied to project root: {latest_wheel.name}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to build wheel: {e}")
        return False
    finally:
        os.chdir(original_dir)

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Build the mtgjson-rust Python extension')
    parser.add_argument('--mode', choices=['debug', 'release'], default='release',
                       help='Build mode (default: release)')
    parser.add_argument('--wheel', action='store_true',
                       help='Build a wheel instead of installing in development mode')
    parser.add_argument('--check-only', action='store_true',
                       help='Only check if required tools are installed')
    
    args = parser.parse_args()
    
    print("MTGJSON Rust Module Builder")
    print("=" * 30)
    
    # Check prerequisites
    if not check_rust_installed():
        sys.exit(1)
    
    if not check_maturin_installed():
        sys.exit(1)
    
    if args.check_only:
        print("\n✓ All required tools are available")
        return
    
    # Build
    if args.wheel:
        success = build_wheel()
    else:
        success = build_rust_module(args.mode)
    
    if success:
        print("\n✓ Build completed successfully!")
        if not args.wheel:
            print("The mtgjson_rust module is now available for import in Python.")
    else:
        print("\n✗ Build failed!")
        sys.exit(1)

if __name__ == '__main__':
    main()