# ingesta.py  (ingesta02)
import os, sys, csv, datetime as dt
import boto3
import mysql.connector as mysql

def env(name, default=None, required=False, cast=lambda v: v):
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        raise RuntimeError(f"Falta la variable de entorno: {name}")
    return cast(v) if v is not None else None

def main():
    # ---- MySQL ----
    host     = env("MYSQL_HOST", "localhost")
    port     = env("MYSQL_PORT", "3306", cast=int)
    user     = env("MYSQL_USER", required=True)
    password = env("MYSQL_PASSWORD", required=True)
    database = env("MYSQL_DB", required=True)
    table    = env("MYSQL_TABLE", required=True)
    query    = env("MYSQL_QUERY", default=f"SELECT * FROM `{table}`")
    chunk    = env("CHUNK_SIZE", "1000", cast=int)

    # ---- Salida / S3 ----
    out_csv  = env("OUTPUT_CSV", f"{table}.csv")
    bucket   = env("S3_BUCKET", required=True)
    prefix   = env("S3_PREFIX", "")
    s3_key   = env("S3_KEY", None)
    region   = env("AWS_REGION", None)

    print("Conectando a MySQL…")
    conn = mysql.connect(host=host, port=port, user=user, password=password, database=database)
    cur = conn.cursor()
    cur.execute(query)

    cols = [c[0] for c in cur.description]
    total = 0
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        while True:
            rows = cur.fetchmany(chunk)
            if not rows:
                break
            w.writerows(rows)
            total += len(rows)

    cur.close(); conn.close()
    print(f"Exportadas {total} filas a {out_csv}")

    if not s3_key:
        ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        pfx = (prefix + "/") if (prefix and not prefix.endswith("/")) else (prefix or "")
        base = os.path.splitext(os.path.basename(out_csv))[0]
        s3_key = f"{pfx}{base or table}_{ts}.csv"

    print(f"Subiendo a s3://{bucket}/{s3_key} …")
    session = boto3.session.Session(region_name=region) if region else boto3.session.Session()
    s3 = session.client("s3")
    s3.upload_file(out_csv, bucket, s3_key)
    print("Ingesta completada ✅")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr); sys.exit(1)
