# Makefile for managing a poetry repository and Jupyter kernel installation

# Define variables
REPO_NAME := $(shell basename $$PWD)
VENV_NAME := $(REPO_NAME)-venv
KERNEL_NAME := $(REPO_NAME)-kernel

# Default target
.PHONY: all
all: install

# Create virtual environment
$(VENV_NAME):
	@echo "Creating virtual environment..."
	poetry config virtualenvs.create true
	poetry install

# Install dependencies
.PHONY: install
install: $(VENV_NAME)
	@echo "Installing dependencies..."
	poetry install

# Create Jupyter kernel
.PHONY: kernel
kernel: $(VENV_NAME)
	@echo "Creating Jupyter kernel..."
	poetry run pip install ipykernel
	poetry run python -m ipykernel install --user --name=$(REPO_NAME)

# Remove virtual environment
.PHONY: clean
clean:
	@echo "Removing virtual environment..."
	poetry env remove $(VENV_NAME)

# Remove Jupyter kernel
.PHONY: clean-kernel
clean-kernel:
	@echo "Removing Jupyter kernel..."
	jupyter kernelspec uninstall $(KERNEL_NAME)

# Help target to display available targets
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  install      : Create virtual environment and install dependencies"
	@echo "  kernel       : Create Jupyter kernel for the repository"
	@echo "  clean        : Remove virtual environment"
	@echo "  clean-kernel : Remove Jupyter kernel"
	@echo "  help         : Show this help message"

