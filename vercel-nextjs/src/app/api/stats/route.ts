import { NextRequest, NextResponse } from "next/server";
import { withClient } from "@/lib/motherduck";

export async function GET(request: NextRequest) {
  const startDate = request.nextUrl.searchParams.get("start");
  const endDate = request.nextUrl.searchParams.get("end");

  if (!startDate || !endDate) {
    return NextResponse.json(
      {
        error:
          "Both 'start' and 'end' query parameters are required. Use YYYY-MM-DD format.",
      },
      { status: 400 }
    );
  }

  const datePattern = /^\d{4}-\d{2}-\d{2}$/;
  if (!datePattern.test(startDate) || !datePattern.test(endDate)) {
    return NextResponse.json(
      { error: "Invalid date format. Use YYYY-MM-DD." },
      { status: 400 }
    );
  }

  try {
    const data = await withClient(async (client) => {
      const result = await client.query(
        `SELECT
          sum(passenger_count)::INTEGER AS total_passengers,
          round(sum(fare_amount), 2) AS total_fare
        FROM nyc.taxi
        WHERE tpep_pickup_datetime >= $1
          AND tpep_pickup_datetime < $2`,
        [`${startDate} 00:00:00`, `${endDate} 00:00:00`]
      );
      return result.rows[0];
    });

    return NextResponse.json({
      start: startDate,
      end: endDate,
      ...data,
    });
  } catch (error) {
    console.error("Failed to fetch stats:", error);
    return NextResponse.json(
      { error: "Failed to fetch stats" },
      { status: 500 }
    );
  }
}
