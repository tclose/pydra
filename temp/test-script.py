from pydra.mark.new_functions import task


@task
def myfunc(x: int, y: int) -> int:
    return x + y


mytask = myfunc(name="mytask", x=1, y=2)

# Would like mypy to know that mytask has an inputs attribute and that it has an int
# attribute 'x', so the linter picks up the incorrect value below
mytask.inputs.x = "bad-value"

mytask()
