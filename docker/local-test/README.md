# PyCDS docker local-test environment

The files in this directory allow you to build and run a test environment
for PyCDS equivalent to that in the GitHub Actions CI.

## Why

1. We are currently running CI tests in a very antiquated environment which 
is difficult if not impossible to reproduce on an up-to-date dev machine. 
Docker containers to the rescue.
1. We could just let the CI do the work, but it can take from 2 to 5 minutes
to run tests ... most of that consumed by setting up the docker container
for the test run.
1. So let's just build that environment once, locally, run it interactively, 
run our tests from inside there, and wow zippy.

## What

1. The image is built with all the contents necessary to install and run PyCDS
and its tests. 
1. But since we want to run our own, local test code, we can't install PyCDS 
from a repo. Instead we do that when the container is started.
1. To facilitate this, we set up a working diretory (WORKDIR) in the image
called `/codebase`. 
1. When we run the image, we (must) mount our local codebase to `/codebase`.
(See Run image).
1. When the container starts (image runs), the script 
`entrypoint.sh` installs the local version of PyCDS. (It also sets up 
and `su`s a non-root user, `test`, because PostgreSQL refuses -- sensibly --
to run as the root user, which is what we are up to this point.)
1. PyCDS is installed in development mode (`-e`) so that changes to the
codebase are immediately. Because we have mounted our codebase to the
container, when we make changes to it (outside the container), those changes
are available inside the container, and vice-versa. Therefore we can use all
our local tools outside the container as normal (which is a shedload easier
than trying to install your IDE inside the container :) ).
1. The vice-versa has a downside, which is that runs of the tests leave
behind a set "orphaned" pytest caches which will cause the next
run of the image to fail if they are not cleaned up first with `py3clean`.
We don't, however mount the codebase read-only because we might want 
some of the effects of the test runs to be written to our local codebase. 
(E.g., redirected output.)

## Pull image

The GitHub Action docker-publish automatically builds the image.
Pull it from Dockerhub:

```
docker pull pcic/pycds:local-test
```

## Run image (container)

Run it from the project root directory:

```
py3clean .
docker run -it -v $(pwd):/codebase pcic/pycds:local-test
```

When the container starts, it installs the local codebase as described above.
After that, you are in interactive mode, in a bash shell, so you can issue 
commands, such as `py.test ....` as normal.

Leave the container running for as long as you want. You can do multiple
rounds of modification and testing using a single container, without
restarting (which was the justification for creating it).

## Build image (manual)

Since this image is built automatically by the GitHub Action docker-publish,
you should not need to do this. However, just in case:

From the _project root directory_ (important Docker context location):

```
docker build -t pycds-local-test -f docker/local-test/Dockerfile .
```

You'll only need to do this once unless the Dockerfile is updated.


