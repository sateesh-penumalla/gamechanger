import React, { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getDailyOpportunities } from '../services/api';
import { ArrowUpDown, ChevronDown, ChevronUp, AlertCircle, CheckCircle2, XCircle, Clock, Search, RotateCcw, ExternalLink } from 'lucide-react';
import { cn, formatCurrency, formatNumber } from '../lib/utils';

const SortableHeader = ({ label, sortKey, currentSort, onSort }) => {
  const isActive = currentSort.key === sortKey;
  return (
    <th
      className="p-2 cursor-pointer hover:bg-foreground/5 transition-colors group select-none text-left"
      onClick={() => onSort(sortKey)}
    >
      <div className="flex items-center gap-1">
        {label}
        <div className={cn(
          "transition-opacity",
          isActive ? "opacity-100" : "opacity-0 group-hover:opacity-40"
        )}>
          {isActive ? (
            currentSort.direction === 'asc' ? <ChevronUp size={10} /> : <ChevronDown size={10} />
          ) : <ArrowUpDown size={10} />}
        </div>
      </div>
    </th>
  );
};

const DailyAnalysisTable = () => {
  const [sort, setSort] = useState({ key: 'entry_ts', direction: 'desc' });

  const { data: opportunities, isLoading, isError } = useQuery({
    queryKey: ['dailyOpportunities'],
    queryFn: getDailyOpportunities,
    refetchInterval: 60000 // Refresh every minute
  });

  const getGoogleFinanceLink = (symbol) => `https://www.google.com/finance/quote/${symbol}:NSE`;

  const sortedData = useMemo(() => {
    if (!opportunities) return [];
    return [...opportunities].sort((a, b) => {
      let valA = a[sort.key];
      let valB = b[sort.key];

      if (valA === undefined) valA = '';
      if (valB === undefined) valB = '';

      if (typeof valA === 'string') valA = valA.toLowerCase();
      if (typeof valB === 'string') valB = valB.toLowerCase();

      if (valA < valB) return sort.direction === 'asc' ? -1 : 1;
      if (valA > valB) return sort.direction === 'asc' ? 1 : -1;
      return 0;
    });
  }, [opportunities, sort]);

  const handleSort = (key) => {
    setSort(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc'
    }));
  };

  const getStatusBadge = (status) => {
    if (status === 'TARGET_HIT') return <span className="text-green-500 flex items-center gap-1"><CheckCircle2 size={10} /> TARGET</span>;
    if (status === 'SL_HIT') return <span className="text-red-500 flex items-center gap-1"><XCircle size={10} /> SL HIT</span>;
    return <span className="text-amber-500 flex items-center gap-1"><Clock size={10} /> OPEN</span>;
  };

  if (isError) return <div className="p-4 text-red-500">Failed to load daily analysis.</div>;

  return (
    <section className="glass rounded-2xl border border-border overflow-hidden mt-6">
      <div className="p-3 border-b border-border flex items-center justify-between bg-foreground/[0.02]">
        <h2 className="text-sm font-black uppercase tracking-widest flex items-center gap-2 text-foreground">
          <Search className="w-4 h-4 text-primary" /> Daily Opportunity Analysis (Replay)
        </h2>
        <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">
          {isLoading ? "Scanning..." : `${opportunities?.length || 0} Opportunities Found`}
        </span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="text-[9px] uppercase tracking-wider text-muted-foreground border-b border-border/50 bg-foreground/[0.01]">
              <SortableHeader label="Time" sortKey="entry_ts" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Symbol" sortKey="symbol" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Type" sortKey="type" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Price" sortKey="price" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Status" sortKey="status" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="PnL %" sortKey="pnl_pct" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Exit Time" sortKey="exit_time" currentSort={sort} onSort={handleSort} />
            </tr>
          </thead>
          <tbody className="text-xs font-medium">
            {isLoading ? (
              <tr><td colSpan="7" className="p-10 text-center animate-pulse text-muted-foreground">Replaying market data...</td></tr>
            ) : sortedData.length === 0 ? (
              <tr><td colSpan="7" className="p-10 text-center text-muted-foreground italic">No opportunities found today.</td></tr>
            ) : (
              sortedData.map((opp, idx) => (
                <tr key={idx} className="hover:bg-foreground/[0.02] border-b border-border/30 last:border-0 group">
                  <td className="p-2 font-mono text-muted-foreground">
                    {new Date(opp.entry_ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false })}
                  </td>
                  <td className="p-2">
                    <a
                      href={getGoogleFinanceLink(opp.symbol)}
                      target="_blank"
                      rel="noreferrer"
                      className="text-blue-500 font-bold hover:text-blue-400 flex items-center gap-1"
                    >
                      <ExternalLink size={10} className="text-primary opacity-50" />
                      {opp.symbol}
                    </a>
                  </td>
                  <td className="p-2">
                    <span className={cn(
                      "px-1.5 py-0.5 rounded text-[9px] font-black uppercase tracking-tight",
                      opp.side === 'LONG' ? "bg-green-500/10 text-green-500" : "bg-red-500/10 text-red-500"
                    )}>
                      {opp.side}
                    </span>
                  </td>
                  <td className="p-2 font-mono">{formatCurrency(opp.price)}</td>
                  <td className="p-2 text-[10px] font-bold">
                    {getStatusBadge(opp.status)}
                  </td>
                  <td className={cn(
                    "p-2 font-mono font-bold",
                    opp.pnl_pct > 0 ? "text-green-500" : opp.pnl_pct < 0 ? "text-red-500" : "text-muted-foreground"
                  )}>
                    {opp.pnl_pct > 0 ? '+' : ''}{opp.pnl_pct}%
                  </td>
                  <td className="p-2 font-mono text-muted-foreground">
                    {opp.exit_time ? new Date(opp.exit_time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false }) : '-'}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
};

export default DailyAnalysisTable;
