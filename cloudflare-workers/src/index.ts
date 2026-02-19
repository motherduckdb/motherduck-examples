import { Client } from "pg";

interface Env {
  MOTHERDUCK_HOST: string;
  MOTHERDUCK_DB: string;
  MOTHERDUCK_TOKEN: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const connectionString = `postgresql://anyusername:${env.MOTHERDUCK_TOKEN}@${env.MOTHERDUCK_HOST}:5432/${env.MOTHERDUCK_DB}?sslmode=require`;
    const client = new Client({ connectionString });

    try {
      await client.connect();
    } catch (err) {
      return Response.json(
        { error: "Connection failed", detail: String(err) },
        { status: 502 }
      );
    }

    try {
      if (url.pathname === "/stats") {
        const startDate = url.searchParams.get("start");
        const endDate = url.searchParams.get("end");

        if (!startDate || !endDate) {
          return Response.json(
            { error: "Both 'start' and 'end' query parameters are required. Use YYYY-MM-DD format." },
            { status: 400 }
          );
        }

        const datePattern = /^\d{4}-\d{2}-\d{2}$/;
        if (!datePattern.test(startDate) || !datePattern.test(endDate)) {
          return Response.json(
            { error: "Invalid date format. Use YYYY-MM-DD." },
            { status: 400 }
          );
        }

        const result = await client.query(
          `SELECT
            sum(passenger_count)::INTEGER AS total_passengers,
            round(sum(fare_amount), 2) AS total_fare
          FROM nyc.taxi
          WHERE tpep_pickup_datetime >= $1
            AND tpep_pickup_datetime < $2`,
          [`${startDate} 00:00:00`, `${endDate} 00:00:00`]
        );

        return Response.json({
          start: startDate,
          end: endDate,
          ...result.rows[0],
        });
      }

      // Default route: sample of recent taxi trips
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

      return Response.json(result.rows);
    } finally {
      await client.end();
    }
  },
};
