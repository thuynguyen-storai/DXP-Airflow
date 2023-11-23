# Python versions and Pyenv

## Python complexity

To be honest, Python is very complex.

![https://imgs.xkcd.com/comics/python_environment.png](https://imgs.xkcd.com/comics/python_environment.png)

Making sure you are running the correct Python is a difficult task.
- As many Linux distro used Python as its core, installing a different version may accidentally break the whole system.
- `apt` is slow with updates, so one may not use the necessary version.

One of my personal favorite solution to this problem is to use [pyenv](https://github.com/pyenv/pyenv).

## Pyenv: brief

### Installation

#### Step 1: Install pyenv

For Homebrew users, on macOS or some Linux distros, Homebrew can be used to efficiently install `pyenv`.

```shell
> brew update
> brew install pyenv
```

For normal users (WSL, Ubuntu, Debian, ... - non Homebrew users)

```shell
> curl https://pyenv.run | bash
```

#### Step 2: Enable pyenv in profile
We need to add pyenv to a **profile** file to enable pyenv everytime we start the terminal

For Zsh users (macOS, ...)
```shell
> echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
> echo '[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.zshrc
> echo 'eval "$(pyenv init -)"' >> ~/.zshrc
```

For Bash, common users (WSL, Ubuntu, Debian, ...)
```shell
> echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
> echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
> echo 'eval "$(pyenv init -)"' >> ~/.bashrc
```
#### Step 3: Restart terminal
Close the terminal (using `exit`?) and re-open it to make all changes in effect.


### Install additional Python versions
To show all available versions of Python **for install**, run:

```shell
> pyenv install -l

# Sample output
Available versions:
  ...
  3.10.11
  3.10.12
  3.10.13
  3.11.0
  3.11-dev
  3.11.1
  3.11.2
```

Let say we want to install Python version 3.10.13, use the command below:

```shell
pyenv install 3.10.13
```

After installation, to show all **installed** versions, use:

```shell
pyenv versions

# Sample output
# * system
#   3.10.13
#   3.11.5 (set by /home/ducth/.pyenv/version)
#   3.11.5/envs/python-lab
#   python-lab --> /home/ducth/.pyenv/versions/3.11.5/envs/python-lab
```

The *start* `*` means the current version (i.e, 3.11.5) is currently being used.

### Switching between Python versions

Pyenv have three level of Python versions to switch between:

- `pyenv shell <versions>`: Switch for just **current shell session** (when close shell, this will be reset)
- `pyenv local <versions>`: Switch when you `cd` (change directory) to a directory.
- `pyenv global <versions>`: Switch **globally**

In this scope of doc, we will use the global options to change Python versions:

```shell
# Before changing the version
> python --version
Python 3.11.6

> pyenv global 3.10.13

# After changing the version
> python --version
Python 3.10.13
```

## Conclusion
Pyenv is very useful when working with Python. You should try to learn more about this in the its [documentation](https://github.com/pyenv/pyenv).