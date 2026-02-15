import { useEffect, useMemo, useState } from "react";

function resolveApiBaseUrl() {
  const configured = import.meta.env.VITE_API_BASE_URL || "http://localhost:18080";
  if (typeof window === "undefined") {
    return configured;
  }

  const uiHost = window.location.hostname;
  if (!uiHost) {
    return configured;
  }

  try {
    const parsed = new URL(configured);
    if (parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1") {
      parsed.hostname = uiHost;
      return parsed.toString().replace(/\/$/, "");
    }
    return configured;
  } catch {
    return configured;
  }
}

const API_BASE_URL = resolveApiBaseUrl();

const initialForm = {
  customer_email: "demo@example.com",
  item_sku: "SKU-CHAIR-01",
  quantity: 1,
  amount_cents: 14990,
};

export default function App() {
  const [form, setForm] = useState(initialForm);
  const [orders, setOrders] = useState([]);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState("");

  const apiLabel = useMemo(() => API_BASE_URL, []);

  const fetchOrders = async () => {
    try {
      const res = await fetch(`${API_BASE_URL}/orders`);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const data = await res.json();
      setOrders(data);
    } catch (err) {
      setError(`Failed to load orders: ${err.message}`);
    }
  };

  useEffect(() => {
    fetchOrders();
    const timer = setInterval(fetchOrders, 2500);
    return () => clearInterval(timer);
  }, []);

  const onSubmit = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError("");

    try {
      const res = await fetch(`${API_BASE_URL}/orders`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...form,
          quantity: Number(form.quantity),
          amount_cents: Number(form.amount_cents),
        }),
      });

      if (!res.ok) {
        const payload = await res.json().catch(() => ({}));
        throw new Error(payload.error || `HTTP ${res.status}`);
      }

      setForm(initialForm);
      await fetchOrders();
    } catch (err) {
      setError(`Create failed: ${err.message}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <main className="layout">
      <section className="panel">
        <h1>Kafka Event-Driven Order Processing</h1>
        <p className="meta">API: {apiLabel}</p>
        <form onSubmit={onSubmit} className="form">
          <label>
            Customer Email
            <input
              value={form.customer_email}
              onChange={(e) => setForm((v) => ({ ...v, customer_email: e.target.value }))}
              required
            />
          </label>
          <label>
            Item SKU
            <input
              value={form.item_sku}
              onChange={(e) => setForm((v) => ({ ...v, item_sku: e.target.value }))}
              required
            />
          </label>
          <label>
            Quantity
            <input
              type="number"
              min="1"
              value={form.quantity}
              onChange={(e) => setForm((v) => ({ ...v, quantity: e.target.value }))}
              required
            />
          </label>
          <label>
            Amount (cents)
            <input
              type="number"
              min="1"
              value={form.amount_cents}
              onChange={(e) => setForm((v) => ({ ...v, amount_cents: e.target.value }))}
              required
            />
          </label>
          <button type="submit" disabled={isSubmitting}>
            {isSubmitting ? "Creating..." : "Create Order"}
          </button>
        </form>
        {error && <p className="error">{error}</p>}
      </section>

      <section className="panel">
        <h2>Orders</h2>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Email</th>
              <th>SKU</th>
              <th>Qty</th>
              <th>Amount</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {orders.map((order) => (
              <tr key={order.id}>
                <td className="mono">{order.id.slice(0, 8)}</td>
                <td>{order.customer_email}</td>
                <td>{order.item_sku}</td>
                <td>{order.quantity}</td>
                <td>{order.amount_cents}</td>
                <td>
                  <span className={`status ${order.status.toLowerCase()}`}>{order.status}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}
