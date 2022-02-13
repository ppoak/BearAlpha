import os
import re
import json
import sys

DEBUG_CONFIG = {
    "version": "0.2.0",
    "configurations": []
}
CODE_RUNNER_HEADER = "#!python -m {}"


def process_file_path_end(path):
    return path + '/' if not path.endswith('/') else path


def debug_config(path):
    files = os.listdir(path)
    path = process_file_path_end(path)
    for file in files:
        if os.path.isfile(path + file) and file.endswith('.py') and not file.startswith('__'):
            c = {
                "name": file[:-3],
                "type": "python",
                "request": "launch",
                "module": path.replace('/', '.') + file[:-3]
            }
            DEBUG_CONFIG["configurations"].append(c)
    with open(".vscode/launch.json", 'w') as f:
        json.dump(DEBUG_CONFIG, f, indent='\t')


def code_runner_header(path):
    files = os.listdir(path)
    path = process_file_path_end(path)
    for file in files:
        if os.path.isfile(path + file) and file.endswith('.py') and not file.startswith('__'):
            header = CODE_RUNNER_HEADER.format(path.replace('/', '.') + file[:-3] + '\n')
            with open(path + file, 'r') as f:
                content = f.read()
            if content.startswith("#!python -m "):
                content = re.sub("#!python -m .*\n", header, content)
            else:
                content = header + '\n' + content
            with open(path + file, 'w') as f:
                f.write(content)

if __name__ == '__main__':
    if sys.argv[1] == 'coderunner':
        paths = [
            "utils", 
            ]
        for path in paths:
            code_runner_header(path)
    elif sys.argv[1] == 'debug':
        paths = [
            "utils",
            'other/amount_concentration',
            "factor/define",
            'factor/utils'
            ]
        for path in paths:
            debug_config(path)
