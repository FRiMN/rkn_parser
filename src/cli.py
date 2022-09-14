import click

from src.conveers import prolongation_resolutions_fetch, prolongation_licenses_fetch, commissioning_licenses_fetch, \
    special_licenses_fetch
from src.reports import prolongation_licenses_csv, \
    commissioning_licenses_csv, commissioning_licenses_push, special_licenses_csv, prolongation_licenses_push

from src.reports import prolongation_resolutions_push, prolongation_resolutions_csv


@click.group()
def cli():
    pass


@cli.command()
@click.option('--start', type=click.DateTime(), help='from date')
@click.option('--end', type=click.DateTime(), help='to date')
@click.option('-p', '--process',
              type=click.Choice(['fetch', 'generate_csv', 'push']))
def prolongation_resolutions(start, end, process):
    if process == 'fetch':
        prolongation_resolutions_fetch(start, end)
    if process == 'push':
        prolongation_resolutions_push(start, end)
    if process == 'generate_csv':
        prolongation_resolutions_csv(start, end)


@cli.command()
@click.option('--start', type=click.DateTime(), help='from date')
@click.option('--end', type=click.DateTime(), help='to date')
@click.option('--ours', type=click.BOOL, help='ours or not')
@click.option('-p', '--process',
              type=click.Choice(['fetch', 'generate_csv', 'push']))
def prolongation_licenses(start, end, ours, process):
    if process == 'fetch':
        prolongation_licenses_fetch(start, end, ours)
    if process == 'push':
        prolongation_licenses_push(start, end, ours)
    if process == 'generate_csv':
        prolongation_licenses_csv(start, end, ours)


@cli.command()
@click.option('--start', type=click.DateTime(), help='from date')
@click.option('--end', type=click.DateTime(), help='to date')
@click.option('-p', '--process',
              type=click.Choice(['fetch', 'generate_csv', 'push']))
@click.option('--ours', type=click.BOOL, help='ours or not', default=True)
def commissioning_licenses(start, end, process, ours):
    if process == 'fetch':
        commissioning_licenses_fetch(start, end, ours)
    if process == 'push':
        commissioning_licenses_push(start, end)
    if process == 'generate_csv':
        commissioning_licenses_csv(start, end, ours)


@cli.command()
@click.option('-p', '--process',
              type=click.Choice(['fetch', 'generate_csv']))
def special_licenses(process):
    if process == 'fetch':
        special_licenses_fetch()
    if process == 'generate_csv':
        special_licenses_csv()


if __name__ == '__main__':
    cli()
