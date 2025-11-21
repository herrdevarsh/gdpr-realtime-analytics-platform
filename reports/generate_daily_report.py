import os
import sys
from datetime import date

import psycopg2

from config import POSTGRES_DSN


def fetch_metrics(conn, table_name: str, target_date: date):
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT event_type, total_events, total_amount
        FROM {table_name}
        WHERE event_date = %s
        ORDER BY event_type;
        """,
        (target_date,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def main():
    # date argument: yyyy-mm-dd, default = today
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today()

    conn = psycopg2.connect(POSTGRES_DSN)

    batch_rows = fetch_metrics(conn, "metrics_batch_daily", target)
    streaming_rows = fetch_metrics(conn, "metrics_streaming_daily", target)

    # Turn lists into dicts for easier diff
    batch_dict = {r[0]: (r[1], float(r[2])) for r in batch_rows}
    streaming_dict = {r[0]: (r[1], float(r[2])) for r in streaming_rows}

    print(f"\n=== Daily Business Report for {target.isoformat()} ===\n")

    total_events = 0
    total_revenue = 0.0

    print("By event type (batch vs streaming):\n")
    print(
        "event_type | batch_events | streaming_events | batch_amount | streaming_amount | events_diff | amount_diff"
    )

    all_event_types = sorted(set(batch_dict.keys()) | set(streaming_dict.keys()))

    for et in all_event_types:
        b_ev, b_amt = batch_dict.get(et, (0, 0.0))
        s_ev, s_amt = streaming_dict.get(et, (0, 0.0))

        events_diff = s_ev - b_ev
        amount_diff = s_amt - b_amt

        print(
            f"{et:<10} | {b_ev:<12} | {s_ev:<16} | {b_amt:<12.2f} | {s_amt:<16.2f} | {events_diff:<11} | {amount_diff:<11.2f}"
        )

        if et == "purchase":
            total_revenue = b_amt  # use batch as source of truth

        total_events += b_ev

    print("\nSummary (from batch metrics):")
    print(f"- Total events: {total_events}")
    print(f"- Total revenue (purchases): {total_revenue:.2f}")

    # Optionally save to a markdown file
    output_dir = os.path.join("reports", "output")
    os.makedirs(output_dir, exist_ok=True)
    outfile = os.path.join(output_dir, f"business_report_{target.isoformat()}.md")

    with open(outfile, "w", encoding="utf-8") as f:
        f.write(f"# Daily Business Report – {target.isoformat()}\n\n")
        f.write("## KPI Summary\n\n")
        f.write(f"- Total events: **{total_events}**\n")
        f.write(f"- Total revenue (purchases): **{total_revenue:.2f}**\n\n")

        f.write("## Batch vs Streaming by Event Type\n\n")
        f.write(
            "| event_type | batch_events | streaming_events | batch_amount | streaming_amount | events_diff | amount_diff |\n"
        )
        f.write("|-----------|--------------|------------------|--------------|------------------|------------|------------|\n")
        for et in all_event_types:
            b_ev, b_amt = batch_dict.get(et, (0, 0.0))
            s_ev, s_amt = streaming_dict.get(et, (0, 0.0))
            events_diff = s_ev - b_ev
            amount_diff = s_amt - b_amt
            f.write(
                f"| {et} | {b_ev} | {s_ev} | {b_amt:.2f} | {s_amt:.2f} | {events_diff} | {amount_diff:.2f} |\n"
            )

    print(f"\nReport saved to {outfile}\n")

    conn.close()


if __name__ == "__main__":
    main()
