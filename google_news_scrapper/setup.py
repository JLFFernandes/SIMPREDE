import os

def setup_project():
    """Setup project directory structure and initial files"""
    
    # Get project root directory
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # Create directory structure
    directories = [
        os.path.join(project_root, "data", "raw"),
        os.path.join(project_root, "data", "structured"),
        os.path.join(project_root, "models"),
        os.path.join(project_root, "logs"),
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Created directory: {directory}")

if __name__ == "__main__":
    setup_project()
    print("✨ Project setup complete!")
