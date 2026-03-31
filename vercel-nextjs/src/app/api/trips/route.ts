import { NextResponse } from "next/server";
import { withClient } from "@/lib/motherduck";

export async function GET() {
  try {
    const rows = await withClient(async (client) => {
      const result = await client.query(
        `SELECT
          tpep_pickup_datetime AS pickup,
          tpep_dropoff_datetime AS dropoff,
          passenger_count,
          trip_distance,
          fare_amount,
          tip_amount,
          total_amount
        FROM nyc.taxi
        ORDER BY tpep_pickup_datetime DESC
        LIMIT 20`
      );
      return result.rows;
    });

    return NextResponse.json(rows);
  } catch (error) {
    console.error("Failed to fetch trips:", error);
    return NextResponse.json(
      { error: "Failed to fetch trips" },
      { status: 500 }
    );
  }
}
