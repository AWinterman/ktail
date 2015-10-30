from kinesis_tail import KinesisClient
import sys
import click

VERSION = '${version}'


@click.group(invoke_without_command=True, help="List all available Kinesis streams")
@click.argument('stream-name', type=click.STRING)
@click.option('--fields', default=False, type=click.STRING, help="Display only given fields from log events")
@click.option('--region', default='eu-west-1', help="Kinesis stream region")
@click.option('--debug', is_flag=True, default=False, help="Debug output")
def tail(stream_name, fields, region, debug):
    try:
        if fields:
            fields = fields.split(',')

        KinesisClient(region).get_json_events_from_stream(stream_name, fields)
    except Exception as e:
        click.echo(e)
        sys.exit(1)


@tail.command(help="List all available Kinesis streams")
@click.option('--region', default='eu-west-1', help="Kinesis stream region")
@click.option('--debug', is_flag=True, default=False, help="Debug output")
def list(region, debug):
    try:

        streams = KinesisClient(region).list_streams()
        for stream in streams:
            click.echo(stream)
    except Exception as e:
        click.echo(e)
        sys.exit(1)


def main():
    tail()


if __name__ == '__main__':
    main()
