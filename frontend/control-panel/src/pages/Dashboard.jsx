import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import ServiceStatusCard from '../components/ServiceStatusCard';
import { getSystemJobs, getDailyFocus, getPositions, executePosition, getOracleResults, triggerOracleSync } from '../services/api';
import { Activity, ExternalLink, TrendingUp, Zap, Target, BarChart3, Clock, RefreshCcw, Compass, ShieldCheck, ShieldAlert } from 'lucide-react';
import { cn } from '../lib/utils';

const MetricCard = ({ title, value, subValue, icon: Icon, trend }) => (
  <div className="glass card-hover p-4 rounded-xl border border-border flex items-center justify-between">
    <div>
      <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest mb-1">{title}</p>
      <div className="flex items-baseline gap-2">
        <h3 className="text-xl font-black text-foreground tracking-tighter">{value}</h3>
        {subValue && <span className="text-[10px] text-muted-foreground font-mono">{subValue}</span>}
      </div>
      {trend && (
        <p className={`text-[10px] font-bold mt-1 ${trend > 0 ? 'text-green-400' : 'text-red-400'}`}>
          {trend > 0 ? '↑' : '↓'} {Math.abs(trend).toFixed(2)}%
        </p>
      )}
    </div>
    <div className="p-3 rounded-xl bg-primary/10 text-primary shadow-[0_0_15px_rgba(59,130,246,0.2)]">
      <Icon size={20} />
    </div>
  </div>
);

const IndicatorBadge = ({ label, value, type = 'default' }) => {
  const getHeatColor = () => {
    if (type === 'rsi') {
      if (value > 70) return 'bg-red-500 shadow-[0_0_10px_rgba(239,68,68,0.4)]';
      if (value < 30) return 'bg-green-500 shadow-[0_0_10px_rgba(34,197,94,0.4)]';
      return 'bg-blue-500/30';
    }
    if (type === 'macd') {
      return value > 0 ? 'bg-green-500/40' : 'bg-red-500/40';
    }
    if (type === 'imbalance') {
      if (value > 0.15) return 'bg-green-500/60 shadow-[0_0_8px_rgba(34,197,94,0.3)]';
      if (value < -0.15) return 'bg-red-500/60 shadow-[0_0_8px_rgba(239,68,68,0.3)]';
      return 'bg-foreground/10';
    }
    if (type === 'iceberg') {
      if (value > 0.7) return 'bg-purple-600 shadow-[0_0_15px_rgba(147,51,234,0.6)] animate-pulse';
      if (value > 0.3) return 'bg-purple-400/60 shadow-[0_0_10px_rgba(147,51,234,0.3)]';
      return 'hidden';
    }
    return 'bg-foreground/10';
  };

  return (
    <div className="flex flex-col items-center gap-1">
      <span className="text-[8px] text-muted-foreground uppercase font-bold tracking-tighter">{label}</span>
      <span className={`px-2 py-0.5 rounded text-[10px] font-mono font-bold ${getHeatColor()} text-white`}>
        {typeof value === 'number' ? value.toFixed(value < 1 ? 3 : 1) : (value || 'N/A')}
      </span>
    </div>
  );
};

const Dashboard = () => {
  const queryClient = useQueryClient();
  const { data: jobs, isLoading: jobsLoading } = useQuery({ queryKey: ['jobs'], queryFn: getSystemJobs, refetchInterval: 5000 });
  const { data: positions, isLoading: posLoading } = useQuery({ queryKey: ['positions'], queryFn: getPositions, refetchInterval: 3000 });
  const { data: oracleResults, isLoading: oracleLoading } = useQuery({ queryKey: ['oracle-results'], queryFn: getOracleResults, refetchInterval: 30000 });

  const executeMutation = useMutation({
    mutationFn: executePosition,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['positions'] });
    },
  });

  const syncOracleMutation = useMutation({
    mutationFn: triggerOracleSync,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['oracle-results'] });
      queryClient.invalidateQueries({ queryKey: ['jobs'] });
    },
  });

  const activeSignals = positions?.filter(p => p.status === 'SIGNALED').length || 0;
  const openPositions = positions?.filter(p => p.status === 'OPEN').length || 0;
  const totalPnL = positions?.reduce((acc, p) => acc + (p.pnl_pct || 0), 0) || 0;

  return (
    <div className="min-h-screen bg-background p-6 space-y-6">
      <header className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-4">
        <div>
          <h1 className="text-3xl font-black tracking-tighter gradient-text uppercase italic">Mimic Command</h1>
          <p className="text-xs text-muted-foreground font-mono flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse" /> LIVE TRADING TERMINAL v4.1.0
          </p>
        </div>
        <div className="flex items-center gap-2 text-foreground/70">
          <div className="glass px-4 py-2 rounded-xl flex items-center gap-3 border border-border">
            <Clock className="w-4 h-4 text-primary" />
            <span className="text-sm font-mono font-bold tracking-tight">
              {new Date().toLocaleTimeString()} <span className="text-muted-foreground opacity-50">IST</span>
            </span>
          </div>
        </div>
      </header>

      <section className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricCard title="Total Signals" value={activeSignals} icon={Zap} subValue="READY TO FIRE" />
        <MetricCard title="Open Positions" value={openPositions} icon={Target} subValue="BOT MANAGED" />
        <MetricCard title="Session PnL" value={`${totalPnL.toFixed(2)}%`} trend={totalPnL} icon={TrendingUp} />
        <MetricCard title="System Load" value="OPTIMAL" icon={Activity} subValue="0.4s LATENCY" />
      </section>

      <section className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {jobsLoading ? (
          <div className="col-span-full h-12 glass flex items-center justify-center rounded-xl">LOAD_SEQUENCE...</div>
        ) : jobs?.map(job => (
          <ServiceStatusCard
            key={job.job_id}
            serviceName={job.job_id.replace('_', ' ')}
            status={job.status}
            jobId={job.job_id}
            lastRun={job.last_run}
          />
        ))}
      </section>

      <div className="space-y-6">
        <section className="glass rounded-2xl border border-border overflow-hidden">
          <div className="p-4 border-b border-border flex items-center justify-between bg-foreground/[0.02]">
            <h2 className="text-sm font-black uppercase tracking-widest flex items-center gap-2 text-foreground">
              <BarChart3 className="w-4 h-4 text-primary" /> Active & Closed Positions
            </h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-left text-muted-foreground border-b border-border uppercase tracking-widest font-black text-[10px]">
                  <th className="p-4">Time</th>
                  <th className="p-4">Instrument</th>
                  <th className="p-4">Execution</th>
                  <th className="p-4 text-center">Indicators</th>
                  <th className="p-4">Status</th>
                  <th className="p-4">PnL</th>
                  <th className="p-4 text-center">Action</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {posLoading ? (
                  <tr><td colSpan="7" className="p-10 text-center animate-pulse tracking-widest">SYNCING_DATA...</td></tr>
                ) : positions?.filter(p => p.status === 'OPEN' || p.status === 'CLOSED').length === 0 ? (
                  <tr><td colSpan="7" className="p-10 text-center text-muted-foreground uppercase font-bold tracking-widest py-20">No active positions found</td></tr>
                ) : positions?.filter(p => p.status === 'OPEN' || p.status === 'CLOSED').map(pos => (
                  <tr key={pos.id} className="hover:bg-foreground/[0.02] transition-colors group">
                    <td className="p-4 font-mono text-muted-foreground whitespace-nowrap">
                      {new Date(pos.entry_time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                    </td>
                    <td className="p-4">
                      <div className="flex flex-col">
                        <a
                          href={`https://www.google.com/finance/quote/${pos.symbol}:NSE`}
                          target="_blank"
                          rel="noreferrer"
                          className="font-black text-sm text-blue-500 hover:text-blue-400 hover:underline flex items-center gap-1"
                        >
                          {pos.symbol} <ExternalLink size={10} className="opacity-50" />
                        </a>
                        <span className={`${pos.side === 'LONG' ? 'text-green-500 bg-green-500/10' : 'text-red-500 bg-red-500/10'} font-bold text-[8px] uppercase tracking-widest px-1.5 py-0.5 rounded w-fit mt-1 border border-current/10`}>
                          {pos.side} • {pos.type || 'ORB'} • {pos.entry_metrics?.direction || 'NEUTRAL'}
                        </span>
                      </div>
                    </td>
                    <td className="p-4 text-xs">
                      <div className="flex flex-col">
                        <div className="flex items-center gap-1.5">
                          <span className="text-foreground font-mono font-bold">₹{pos.entry_price?.toFixed(2)}</span>
                          <span className="text-[10px] text-muted-foreground">→</span>
                          <span className={cn(
                            "text-sm font-black font-mono",
                            pos.live_price > pos.entry_price ? "text-green-500" : pos.live_price < pos.entry_price ? "text-red-500" : "text-foreground"
                          )}>
                            ₹{pos.live_price?.toFixed(2)}
                          </span>
                        </div>
                        <span className="text-[9px] text-muted-foreground uppercase font-medium">Entry → Live</span>
                      </div>
                    </td>
                    <td className="p-4">
                      <div className="flex justify-center gap-2">
                        <IndicatorBadge label="RSI" value={pos.entry_metrics?.rsi} type="rsi" />
                        <IndicatorBadge label="MACD" value={pos.entry_metrics?.macd} type="macd" />
                        <IndicatorBadge label="SURGE" value={pos.entry_metrics?.vol_surge} />
                        <IndicatorBadge label="B/A FLOW" value={pos.imbalance} type="imbalance" />
                        <IndicatorBadge label="ICEBERG" value={pos.iceberg_score} type="iceberg" />
                      </div>
                    </td>
                    <td className="p-4">
                      <span className={cn(
                        "px-2 py-1 rounded text-[9px] font-black uppercase tracking-widest border",
                        pos.status === 'OPEN' ? 'bg-blue-500/10 text-blue-500 border-blue-500/20 shadow-[0_0_10px_rgba(59,130,246,0.1)]' :
                          'bg-muted text-muted-foreground border-border'
                      )}>
                        {pos.status}
                      </span>
                    </td>
                    <td className="p-4">
                      <span className={cn(
                        "font-mono font-black text-sm transition-all group-hover:px-2 group-hover:rounded group-hover:bg-foreground/5",
                        pos.pnl_pct > 0 ? "text-green-500" : pos.pnl_pct < 0 ? "text-red-500" : "text-foreground/30"
                      )}>
                        {pos.pnl_pct ? (pos.pnl_pct > 0 ? `+${pos.pnl_pct.toFixed(2)}%` : `${pos.pnl_pct.toFixed(2)}%`) : '0.00%'}
                      </span>
                    </td>
                    <td className="p-4 text-center">
                      <span className="text-[9px] text-muted-foreground/50 border border-border px-2 py-1 rounded uppercase tracking-widest font-bold">
                        PROTECTED
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        {/* ORACLE INTELLIGENCE PANEL */}
        <section className="glass rounded-2xl border border-border overflow-hidden">
          <div className="p-4 border-b border-border flex items-center justify-between bg-foreground/[0.02]">
            <h2 className="text-sm font-black uppercase tracking-widest flex items-center gap-2 text-foreground">
              <Compass className="w-4 h-4 text-purple-500" /> Oracle Intelligence
            </h2>
            <button
              onClick={() => syncOracleMutation.mutate()}
              disabled={syncOracleMutation.isPending}
              className="p-1.5 rounded-lg bg-foreground/5 hover:bg-foreground/10 text-muted-foreground hover:text-foreground transition-all disabled:opacity-50"
            >
              <RefreshCcw className={cn("w-3.5 h-3.5", syncOracleMutation.isPending && "animate-spin")} />
            </button>
            <div className="ml-2 text-[9px] font-mono text-muted-foreground text-right leading-tight">
              <p className="uppercase font-bold opacity-50">Last Updated</p>
              <p>{oracleResults?.last_updated ? new Date(oracleResults.last_updated).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', day: 'numeric', month: 'short' }) : '-'}</p>
            </div>
          </div>
          <div className="p-6">
            {oracleLoading ? (
              <div className="animate-pulse flex gap-6">
                <div className="h-40 bg-foreground/5 rounded-xl flex-1" />
                <div className="h-40 bg-foreground/5 rounded-xl flex-1" />
                <div className="h-40 bg-foreground/5 rounded-xl flex-1" />
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                {/* Market Bias Summary */}
                <div className="space-y-4">
                  <p className="text-[10px] font-black text-muted-foreground uppercase tracking-widest">Market Bias Summary</p>
                  <div className="grid grid-cols-2 gap-3">
                    <div className="p-4 rounded-2xl bg-green-500/5 border border-green-500/10 text-center">
                      <p className="text-[9px] font-bold text-green-500 uppercase mb-1">UP Snipers</p>
                      <p className="text-2xl font-black text-foreground">{oracleResults?.summary?.up_snipers || 0}</p>
                    </div>
                    <div className="p-4 rounded-2xl bg-red-500/5 border border-red-500/10 text-center">
                      <p className="text-[9px] font-bold text-red-500 uppercase mb-1">DOWN Snipers</p>
                      <p className="text-2xl font-black text-foreground">{oracleResults?.summary?.down_snipers || 0}</p>
                    </div>
                  </div>
                </div>

                {/* Top Bullish Bias */}
                <div className="space-y-3">
                  <p className="text-[10px] font-black text-muted-foreground uppercase tracking-widest flex items-center gap-2">
                    <ShieldCheck className="w-3.5 h-3.5 text-green-500" /> Top Bullish Bias
                  </p>
                  <div className="space-y-1.5">
                    {oracleResults?.details?.up?.slice(0, 5).map(stock => (
                      <div key={stock.symbol} className="px-3 py-2.5 rounded-xl bg-foreground/[0.02] border border-border flex items-center justify-between">
                        <span className="text-xs font-black text-blue-500">{stock.symbol}</span>
                        <div className="flex gap-4 text-[10px] font-mono">
                          <span className="text-muted-foreground font-bold">RSI <span className="text-green-500">{stock.rsi}</span></span>
                          <span className="text-muted-foreground font-bold">SMA <span className="text-foreground">{stock.sma}</span></span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Top Bearish Bias */}
                <div className="space-y-3">
                  <p className="text-[10px] font-black text-muted-foreground uppercase tracking-widest flex items-center gap-2">
                    <ShieldAlert className="w-3.5 h-3.5 text-red-500" /> Top Bearish Bias
                  </p>
                  <div className="space-y-1.5">
                    {oracleResults?.details?.down?.slice(0, 5).map(stock => (
                      <div key={stock.symbol} className="px-3 py-2.5 rounded-xl bg-foreground/[0.02] border border-border flex items-center justify-between">
                        <span className="text-xs font-black text-red-500">{stock.symbol}</span>
                        <div className="flex gap-4 text-[10px] font-mono">
                          <span className="text-muted-foreground font-bold">RSI <span className="text-red-500">{stock.rsi}</span></span>
                          <span className="text-muted-foreground font-bold">SMA <span className="text-foreground">{stock.sma}</span></span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </div>
        </section>
      </div>
    </div>
  );
};

export default Dashboard;
