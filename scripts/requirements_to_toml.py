from pathlib import Path
import tomllib
# Use: Pycharm > Tools > Sync Python requirements
# THis creates a complex file with a weird formatting. Oepn it manually, copy and paste it to a new clean requirements.txt file
# then execute this script
# then copy paste dependenices.txt content into pyproject.toml
requirements_path = Path(r'C:\Users\thano\Desktop\exso\requirements.txt')
output_path = requirements_path.parent / 'dependencies.txt'
with open(requirements_path, 'r') as f:
    reqs = f.read().split('\n')
dependencies = []
for req in reqs:
    print(req)
    if req:
        package, version = req.split('==')
        dependency = '"' + package + '>=' + version + '"'
        dependencies.append(dependency)

# pyproj = r'C:\Users\thano\Desktop\exso\pyproject.toml'
# with open(pyproj, 'rb') as f:
#     proj = tomllib.load(f)
# proj['project']['dependencies'] = dependencies
#
with open(output_path, 'w') as f:
    f.write("dependencies = [\n")
    for d in dependencies:
        f.write('  ' + d + ',\n')
    f.write(']')

print(dependencies)



# print(reqs)