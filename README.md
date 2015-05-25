# itab
Python tab files parsing and validating schema tools.

## Install
```bash
pip install --upgrade git+https://github.com/bbglab/itab.git
```

## Usage example
```python
import itab

reader = itab.open(file)

for row, errors in reader:
  if len(errors) > 0:
    print("[line {}] ERRORS: {}".format(reader.num_line, errors))
    continue
  else:
    for cell in row:
      print(cell)
``` 

