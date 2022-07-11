try:
    import psycopg
except ImportError:
    pass
else:
    from .inputs import PostgresInput
    from .outputs import PostgresOutput
