### Data Pipeline Overview

1. A **Python script** collects the data.
2. The collected data is stored in a **PostgreSQL database**.
3. A **Node.js API** fetches the data from PostgreSQL.
4. The fetched data is converted into a **Parquet file**.
5. Finally, the Parquet file is **uploaded to storage**.
