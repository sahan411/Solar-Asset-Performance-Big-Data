import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export interface PowerChartPoint {
  timestamp: string;
  currentPowerKw: number;
  expectedPowerKw: number | null;
}

interface PowerChartProps {
  data: PowerChartPoint[];
}

function formatTick(timestamp: string): string {
  return new Date(timestamp).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

// Actual is always plotted. Expected only appears when the API has actually
// returned a value for every point — a partially-null series would draw a
// misleading broken expected line, so it is omitted entirely rather than
// interpolated or zero-filled.
export function PowerChart({ data }: PowerChartProps) {
  const hasExpected = data.length > 0 && data.every((point) => point.expectedPowerKw !== null);

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
        <XAxis dataKey="timestamp" tickFormatter={formatTick} stroke="var(--color-text-muted)" fontSize={12} />
        <YAxis stroke="var(--color-text-muted)" fontSize={12} unit=" kW" width={70} />
        <Tooltip
          labelFormatter={(value) => new Date(value as string).toLocaleTimeString()}
          formatter={(value: number, name: string) => [`${value.toFixed(1)} kW`, name]}
        />
        <Legend />
        <Line
          type="monotone"
          dataKey="currentPowerKw"
          name="Actual power"
          stroke="var(--color-accent)"
          dot={false}
          strokeWidth={2}
          isAnimationActive={false}
        />
        {hasExpected && (
          <Line
            type="monotone"
            dataKey="expectedPowerKw"
            name="Expected power"
            stroke="var(--color-text-muted)"
            strokeDasharray="4 4"
            dot={false}
            strokeWidth={2}
            isAnimationActive={false}
          />
        )}
      </LineChart>
    </ResponsiveContainer>
  );
}
