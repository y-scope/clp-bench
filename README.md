# clp-bench
clp-bench is a tool for benchmarking [CLP] as well as other log management tools. The tool itself is
a Python package and we also provide a [web interface][ui] for viewing results.

## Requirements

* Docker
* Python v3.10 or higher

# Set up

```shell
python3 -m venv venv
. venv/bin/activate
pip install -e .
```

You can use `clp-bench --help` to see usage instructions.

[CLP]: https://github.com/y-scope/clp
[ui]: ui
