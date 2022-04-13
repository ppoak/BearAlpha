import os
import json


DEBUG_CONFIG = {
    "version": "0.2.0",
    "configurations": [
        {
			"name": "current file",
			"type": "python",
			"request": "launch",
			"program": "${file}",
			"console": "integratedTerminal",
			"justMyCode": True
		}
        ]
}

all_files = []
for dirs, _, file_list in os.walk('.'):
    for file in file_list:
        all_files.append(os.path.join(dirs, file))

py_files = filter(lambda x: x.endswith('.py'), all_files)
pymodules = []
for pyfile in py_files:
    pyfile = pyfile[2:-3]
    if pyfile.startswith('.'):
        continue
    pymodule = pyfile.replace('/', '.')
    pymodules.append(pymodule)
    config = {
        "name": pyfile,
        "type": "python",
        "request": "launch",
        "module": pymodule
    }
    DEBUG_CONFIG['configurations'].append(config)

with open(".vscode/launch.json", 'w') as f:
    json.dump(DEBUG_CONFIG, f, indent='\t')
