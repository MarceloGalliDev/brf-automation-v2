# brf-automation-v2
 automação brf

# Virtual ENV
- pipenv --python 3.10.6
- ativar shell = pipenv shell
- instalar pacotes dev = pipenv install <package_name> --dev
- instalar pacotes = pipenv install <package_name>
- executar script sem o shell pipenv = pipenv run python script_name.py
- remover ambiente virtual = pipenv --rm
- ver dependencias = pipenv graph
- verificar vulnerability = pipenv check

# Eleminando path duplicado
### Get current PYTHONPATH and convert to an array
$paths = $env:PYTHONPATH -split ';'

### Get unique paths
$uniquePaths = $paths | Get-Unique

### Convert the array back to a string
$newPythonPath = $uniquePaths -join ';'

### Set the new PYTHONPATH
$env:PYTHONPATH = $newPythonPath

### Print the new PYTHONPATH
$env:PYTHONPATH
