
### Backup database

**Including data**

```bash
pg_dump -h localhost -p 5432 -U postgres -W postgres > dump_2024-01-01.sql 
```

**Schema only**

```bash
pg_dump -h localhost -p 5432 -U postgres -W -s postgres > schema.sql
```
