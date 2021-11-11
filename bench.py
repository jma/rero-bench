#!/usr/bin/env python

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import Column, Integer, String, JSON
from sqlalchemy.dialects.postgresql import JSONB
# from sqlalchemy_utils.types import JSONType
import json
import random
import string

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
@click.argument('connection')
@click.pass_context
def database(ctx, connection, number):
    """Simple program that greets NAME for a total of COUNT times."""
    message(f'Connecting to {connection} {ctx.obj["DEBUG"]}')

    engine = create_engine(connection, echo=ctx.obj['DEBUG'])
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    message(f'Create {number} data')
    with click.progressbar(range(number)) as bar:
        for n in bar:
            data = Data(id=n, json={'name': get_random_string(15)})
            session.add(data)
        session.commit()

    message(f'Get {number} data')
    with click.progressbar(session.query(Data).all()) as bar:
        for data in bar:
            metadata = data.json

    message(f'Update {number} data')
    with click.progressbar(session.query(Data).all()) as bar:
        for data in bar:
            metadata = data.json
            metadata['name'] = get_random_string(15)
            data.json = metadata
            flag_modified(data, 'json')
            session.merge(data)
        session.commit()

    message(f'Delete {number} data')
    with click.progressbar(session.query(Data).all()) as bar:
        for data in bar:
            session.delete(data)
        session.commit()

if __name__ == '__main__':
    cli()
