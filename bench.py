#!/usr/bin/env python

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import Column, Integer, String, JSON
from sqlalchemy.dialects.postgresql import JSONB
import yaml

# from sqlalchemy_utils.types import JSONType
import json
import random
import string
from timeit import timeit

def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

Base = declarative_base()

class Data(Base):
    __tablename__ = 'bench_data'
    id = Column(Integer, primary_key=True)
    json = Column(
        JSON().with_variant(
            JSONB(none_as_null=True),
            'postgresql',
        ).with_variant(
            JSON(),
            'sqlite',
        ).with_variant(
            JSON(),
            'mysql',
        ),
        default=lambda: dict(),
        nullable=True
    )

    def __repr__(self):
        data = {
            'id': self.id,
            'metadata': self.json
        }
        return json.dumps(data)

def db_benchmark(connection, number, debug=False):
    message(f'Connecting to {connection}')
    engine = create_engine(connection, echo=debug)

    Session = sessionmaker(bind=engine)
    session = Session()
    db_version = 'n.a.'
    try:
        db_version = session.execute('SELECT version();').first()[0]
    except:
        pass

    table = Data.metadata.tables.get(Data.__tablename__)
    try:
        table.drop(bind=engine)
    except:
        pass
    table.create(bind=engine)
    durations = dict()
    total_duration = 0
    def create_rows_in_db():
        message(f'Create {number} data')
        with click.progressbar(range(number)) as bar:
            for n in bar:
                data = Data(id=n, json={'name': get_random_string(15)})
                session.add(data)
            session.commit()
    duration = timeit(create_rows_in_db, number=1)
    total_duration += duration
    durations['create'] = duration
    click.secho(f'Create duration: {duration}s', fg='green')

    def fast_update_rows_in_db():
        message(f'Fast update {number} data')
        session.query(Data).update({Data.json: {'name': 'new_name'}})
        session.commit()
    duration = timeit(fast_update_rows_in_db, number=1)
    durations['fast update'] = duration
    total_duration += duration
    click.secho(f'Fast update duration: {duration}s', fg='green')

    def get_rows_in_db():
        message(f'Get {number} data')
        with click.progressbar(session.query(Data).all()) as bar:
            for data in bar:
                metadata = data.json
    duration = timeit(get_rows_in_db, number=1)
    durations['get'] = duration
    total_duration += duration
    click.secho(f'Get duration: {duration}s', fg='green')

    def update_rows_in_db():
        message(f'Update {number} data')
        with click.progressbar(session.query(Data).all()) as bar:
            for data in bar:
                metadata = data.json
                metadata['name'] = get_random_string(15)
                data.json = metadata
                flag_modified(data, 'json')
                session.merge(data)
            session.commit()
    duration = timeit(update_rows_in_db, number=1)
    durations['update'] = duration
    total_duration += duration
    click.secho(f'Update duration: {duration}s', fg='green')


    def delete_rows_in_db():
        message(f'Delete {number} data')
        with click.progressbar(session.query(Data).all()) as bar:
            for data in bar:
                session.delete(data)
            session.commit()
    duration = timeit(delete_rows_in_db, number=1)
    durations['delete'] = duration
    total_duration += duration
    click.secho(f'Delete duration: {duration}s', fg='green')

    durations['total'] = total_duration
    click.secho(f'Total duration: {total_duration}s', fg='green')
    table.drop(bind=engine)
    return db_version, durations


def message(msg):
    click.secho(f' {msg}    ', fg='white', bg='blue')
@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def cli(ctx, debug):
    # click.echo(f"Debug mode is {'on' if debug else 'off'}")
    ctx.ensure_object(dict)

    ctx.obj['DEBUG'] = debug

@cli.command()
@click.option('--number', default=100, help='Number of data.')
@click.argument('configuration', type=click.File('rb'))
@click.argument('output', type=click.File('w'))
@click.pass_context
def database(ctx, configuration, output, number):
    """Simple program that greets NAME for a total of COUNT times."""
    config = yaml.load(configuration, Loader=yaml.Loader)
    for name, cfg in config.items():
        if not cfg.get('db'):
            click.secho(f'Wrong config format: db property is missing for "{name}"', bg='red')
            raise click.Abort()
    columns = None
    for name, cfg in config.items():
        message(f'Processing: {name}')
        db_version, results = db_benchmark(cfg['db'], number, debug=ctx.obj['DEBUG'])
        if not columns:
            columns = list(results.keys())
            output.write(f'| name | version | {" | ".join(columns)} |\n')
            output.write(f'| ---- | ---- | {" | ".join(["----" for c in columns])} |\n')
            first = False
        values = ["{0:.2f}".format(results[col]) for col in columns]
        output.write(f'| {name} | {db_version} | {" | ".join(values)} |\n')

if __name__ == '__main__':
    cli()
