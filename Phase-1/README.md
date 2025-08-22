# Interfolio Data Collector

Phase 1: Data Gathering - Pulls users, publications, and grants from the Interfolio API and stores them in PostgreSQL.

## Setup

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment variables in `.env`:

   ```
   DATABASE_URL=postgresql://admin:secret@localhost/research
   API_PUBLIC_KEY=your_interfolio_public_key
   API_PRIVATE_KEY=your_interfolio_private_key
   TENANT_1_DATABASE_ID=your_database_id
   ```

3. Create database tables:

   ```bash
   python create_db.py
   ```

4. Run data collection:

   ```bash
   python gather_data.py
   ```

### Create PostgreSQL user and database

```sh
createuser -s admin
createdb -O admin research
psql -d postgres -c "ALTER USER admin PASSWORD 'secret';"
```

## Features

- **Multi-threaded processing**: Uses 16 cores efficiently for concurrent API calls
- **SQLAlchemy ORM**: Clean database models and operations
- **Error handling**: Robust error handling with statistics tracking
- **Data types supported**: Journal Articles, Books, and Grants
- **Duplicate handling**: Uses merge operations to handle existing records

## Database Schema

- **users**: User profiles from Interfolio
- **publications**: Journal articles and books  
- **grants**: Grant funding information

All linked by foreign key relationships with proper indexing.

<br>
