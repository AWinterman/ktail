from kinesis_tail import KinesisClient
import kinesis_tail
import sys
import click


@click.command(help="List all available Kinesis streams")
@click.argument('stream-name', envvar='KTAIL_STREAM_NAME', default=False, type=click.STRING, required=False)
@click.option('--fields', envvar='KTAIL_FIELDS', default=False, type=click.STRING,
              help="Display only given fields from log events")
@click.option('--region', envvar='KTAIL_REGION', default='eu-west-1', help="Kinesis stream region")
@click.option('--debug', is_flag=True, default=False, help="Debug output")
@click.version_option(version=kinesis_tail.__version__)
def tail(stream_name, fields, region, debug):
    try:
        if stream_name:
            if fields:
                fields = fields.split(',')

            KinesisClient(region, debug=debug).get_json_events_from_stream(stream_name, fields)
        else:
            streams = KinesisClient(region, debug=debug).list_streams()
            click.echo("Available streams:")
            for stream in streams:
                click.echo(stream)
    except Exception as e:
        click.echo(e)
        sys.exit(1)


def main():
    tail()


if __name__ == '__main__':
    main()
