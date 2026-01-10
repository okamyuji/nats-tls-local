import { useEffect, useRef, useState } from 'react'
import { Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import './App.css'

interface MetricsData {
  timestamp: string
  total_logs: number
  by_level: Record<string, number>
  by_service: Record<string, number>
  avg_error_rate: number
  samples: number
}

interface Alert {
  timestamp: string
  severity: string
  source: string
  title: string
  message: string
  context?: Record<string, unknown>
}

interface WSMessage {
  subject: string
  data: MetricsData | Alert
}

function App() {
  const [connected, setConnected] = useState(false)
  const [metrics, setMetrics] = useState<MetricsData | null>(null)
  const [metricsHistory, setMetricsHistory] = useState<Array<{ time: string; errorRate: number; totalLogs: number }>>([])
  const [alerts, setAlerts] = useState<Alert[]>([])
  const [levelData, setLevelData] = useState<Array<{ name: string; count: number }>>([])
  const [serviceData, setServiceData] = useState<Array<{ name: string; count: number }>>([])
  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimeoutRef = useRef<number | null>(null)

  useEffect(() => {
    const connect = () => {
      const wsUrl = import.meta.env.VITE_WS_URL || 'ws://localhost:8080/ws'
      const ws = new WebSocket(wsUrl)
      wsRef.current = ws

      ws.onopen = () => {
        console.log('Connected to WebSocket')
        setConnected(true)
      }

      ws.onclose = () => {
        console.log('Disconnected from WebSocket')
        setConnected(false)
        reconnectTimeoutRef.current = window.setTimeout(connect, 3000)
      }

      ws.onerror = (error) => {
        console.error('WebSocket error:', error)
      }

      ws.onmessage = (event) => {
        try {
          const msg: WSMessage = JSON.parse(event.data)

          if (msg.subject.startsWith('dashboard.')) {
            const rawData = msg.data as unknown as { type?: string; data?: MetricsData }
            const data = rawData.data || rawData as unknown as MetricsData
            setMetrics(data)

            // Update history
            setMetricsHistory(prev => {
              const time = new Date().toLocaleTimeString()
              const newEntry = {
                time,
                errorRate: (data.avg_error_rate || 0) * 100,
                totalLogs: data.total_logs || 0
              }
              return [...prev.slice(-30), newEntry]
            })

            // Update level data
            if (data.by_level) {
              setLevelData(
                Object.entries(data.by_level).map(([name, count]) => ({ name, count }))
              )
            }

            // Update service data
            if (data.by_service) {
              setServiceData(
                Object.entries(data.by_service).map(([name, count]) => ({ name, count }))
              )
            }
          } else if (msg.subject.startsWith('alerts.')) {
            const alert = msg.data as Alert
            setAlerts(prev => [alert, ...prev.slice(0, 49)])
          }
        } catch (e) {
          console.error('Failed to parse message:', e)
        }
      }
    }

    connect()

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current)
      }
      if (wsRef.current) {
        wsRef.current.close()
      }
    }
  }, [])

  const getSeverityClass = (severity: string) => {
    switch (severity) {
      case 'critical': return 'severity-critical'
      case 'error': return 'severity-error'
      case 'warning': return 'severity-warning'
      default: return ''
    }
  }

  return (
    <div className="dashboard">
      <header className="header">
        <h1>Log Pipeline Dashboard</h1>
        <div className={`connection-status ${connected ? 'connected' : 'disconnected'}`}>
          {connected ? '● Connected' : '○ Disconnected'}
        </div>
      </header>

      <main className="main">
        {/* Stats Overview */}
        <section className="stats-grid">
          <div className="stat-card">
            <div className="stat-label">Total Logs</div>
            <div className="stat-value">{metrics?.total_logs?.toLocaleString() || 0}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Error Rate</div>
            <div className={`stat-value ${(metrics?.avg_error_rate || 0) > 0.1 ? 'error' : ''}`}>
              {((metrics?.avg_error_rate || 0) * 100).toFixed(2)}%
            </div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Services</div>
            <div className="stat-value">{Object.keys(metrics?.by_service || {}).length}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Recent Alerts</div>
            <div className="stat-value">{alerts.length}</div>
          </div>
        </section>

        {/* Charts */}
        <section className="charts-grid">
          <div className="chart-card">
            <h3>Error Rate Over Time</h3>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={metricsHistory}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                <XAxis dataKey="time" stroke="#888" fontSize={10} />
                <YAxis stroke="#888" fontSize={10} unit="%" />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1a1a2e', border: '1px solid #333' }}
                  labelStyle={{ color: '#fff' }}
                />
                <Line
                  type="monotone"
                  dataKey="errorRate"
                  stroke="#ff6b6b"
                  strokeWidth={2}
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>

          <div className="chart-card">
            <h3>Logs by Level</h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={levelData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                <XAxis dataKey="name" stroke="#888" fontSize={10} />
                <YAxis stroke="#888" fontSize={10} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1a1a2e', border: '1px solid #333' }}
                  labelStyle={{ color: '#fff' }}
                />
                <Bar dataKey="count" fill="#4ecdc4" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="chart-card">
            <h3>Logs by Service</h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={serviceData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                <XAxis type="number" stroke="#888" fontSize={10} />
                <YAxis dataKey="name" type="category" stroke="#888" fontSize={10} width={100} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1a1a2e', border: '1px solid #333' }}
                  labelStyle={{ color: '#fff' }}
                />
                <Bar dataKey="count" fill="#a78bfa" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </section>

        {/* Alerts */}
        <section className="alerts-section">
          <h3>Recent Alerts</h3>
          <div className="alerts-list">
            {alerts.length === 0 ? (
              <div className="no-alerts">No alerts yet</div>
            ) : (
              alerts.map((alert, index) => (
                <div key={index} className={`alert-item ${getSeverityClass(alert.severity)}`}>
                  <div className="alert-header">
                    <span className="alert-severity">{alert.severity.toUpperCase()}</span>
                    <span className="alert-title">{alert.title}</span>
                    <span className="alert-time">
                      {new Date(alert.timestamp).toLocaleTimeString()}
                    </span>
                  </div>
                  <div className="alert-message">{alert.message}</div>
                </div>
              ))
            )}
          </div>
        </section>
      </main>
    </div>
  )
}

export default App
