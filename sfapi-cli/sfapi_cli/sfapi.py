#!/usr/bin/env python3
from sfapi_client import Client
from sfapi_client.compute import Machine
from sfapi_client.jobs import JobCommand
from typing import Optional
import typer
import json
import logging


app = typer.Typer()


def print_json(*args, **kwargs):
    if isinstance(args[0], list):
        print(json.dumps([j.dict() for j in args[0]], default=str))
    else:
        try:
            print(json.dumps(args[0].dict(), default=str))
        except Exception:
            print(json.dumps(args[0], default=str))


@app.callback()
def main(
    ctx: typer.Context,
    debug: bool = typer.Option(False, help="Print out the logging debug"),
    site: Optional[Machine] = typer.Option(
        "perlmutter", "-s", "--site", help="Site to use"
    ),
):
    if debug:
        logging.basicConfig(encoding="utf-8", level=logging.DEBUG)
    else:
        logging.basicConfig(encoding="utf-8", level=logging.ERROR)

    client = Client(api_base_url="https://api.nersc.gov/api/v1.2")
    compute = client.compute(site)
    ctx.obj = [client, compute]


@app.command()
def cat(
    ctx: typer.Context,
    path: str = typer.Option(None, "-p", "--path", help="Path at NERSC")
):
    [client, compute] = ctx.obj
    [ret] = compute.ls(path)
    with ret.open('r') as fl:
        print(fl.read())


@app.command()
def hostname(ctx: typer.Context):
    [client, compute] = ctx.obj
    ret = compute.run("hostname")
    print(ret)


@app.command()
def token(ctx: typer.Context):
    [client, compute] = ctx.obj
    print(client.token)


@app.command()
def status(ctx: typer.Context):
    [client, compute] = ctx.obj
    print_json(compute)


@app.command()
def ls(
    ctx: typer.Context,
    path: str = typer.Option(None, "-p", "--path", help="Path at NERSC"),
):
    [client, compute] = ctx.obj
    if path is None:
        user = client.user()
        path = f"/global/homes/{user.name[0]}/{user.name}/"
    ret = compute.ls(path)
    print_json(ret)


@app.command()
def jobs(
    ctx: typer.Context,
    user: str = typer.Option(None, "-u", "--user",
                             help="Username to get jobs for"),
    command: Optional[JobCommand] = typer.Option(
        "squeue", "-c", help="Command used to get job info"),
):
    [client, compute] = ctx.obj
    ret = compute.jobs(user=user, command=command)
    print_json(ret)


@app.command()
def job(
    ctx: typer.Context,
    jobid: str = typer.Option(None, "-j", "--jobid",
                              help="Job id to get info for"),
    command: Optional[JobCommand] = typer.Option(
        "sacct", "-c", help="Command used to get job info"
    ),
):
    [client, compute] = ctx.obj
    ret = compute.job(jobid=jobid, command=command)
    print_json(ret)


@app.command()
def submit_job(
    ctx: typer.Context,
    path: str = typer.Option(
        ..., "--path", "-p", help="Path to slurm submit file at NERSC"
    ),
):
    [client, compute] = ctx.obj
    remote_path = compute.ls(path)
    if len(remote_path) == 0:
        print("Not found")
    elif len(remote_path) > 1:
        print("Is this dir")
    else:
        ret = compute.submit_job(path)
        print_json(ret)


@app.command()
def cancel_job(
    ctx: typer.Context,
    jobid: int = typer.Option(..., "--jobid", "-j", help="jobid to cancel"),
):
    [client, compute] = ctx.obj
    # Get job object
    job = compute.job(jobid=jobid, command=JobCommand.sacct)
    # Cancel job
    ret = job.cancel()
    print_json(ret)


if __name__ == "__main__":
    app()
