#!/bin/bash

# Function to initialize the distributed system
initialize_system() {
    echo "Initializing distributed system..."
    python start.py
}

# Function to run other test code
run_tests() {
    echo "Running test code..."
    python test_basic.py
}

# Main script

# Run initialization
initialize_system

# Run tests
run_tests
