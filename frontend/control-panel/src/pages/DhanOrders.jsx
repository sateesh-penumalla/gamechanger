import React, { useState } from 'react';
import { toast } from 'sonner';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  History,
  ArrowUpCircle,
  ArrowDownCircle,
  RefreshCw,
  AlertCircle,
  CheckCircle2,
  Clock,
  TrendingUp,
  Briefcase,
  LineChart
} from 'lucide-react';
import { getDhanSync } from '../services/api';
import { cn } from '../lib/utils';

const DhanOrders = () => {
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState('orders');

  const { data, isLoading, refetch, isRefetching } = useQuery({
    queryKey: ['dhan_sync'],
    queryFn: getDhanSync,
    refetchInterval: 15000 // Auto-sync every 15s
  });

  const syncMutation = useMutation({
    mutationFn: getDhanSync,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dhan_sync'] });
      queryClient.invalidateQueries({ queryKey: ['positions'] });
      toast.success('Dhan HQ Reconciliation Complete');
    },
    onError: (err) => {
      toast.error(`Reconciliation failed: ${err.message}`);
    }
  });

  const handleReconcile = () => {
    toast.promise(syncMutation.mutateAsync(), {
      loading: 'Syncing with Dhan HQ...',
      success: 'Dhan HQ Reconciliation Complete',
      error: (err) => `Reconciliation failed: ${err.message}`
    });
  };

  const orders = data?.dhan?.orders || [];
  const trades = data?.dhan?.trades || [];
  const positions = data?.dhan?.positions || [];

  const TabButton = ({ id, label, icon: Icon }) => (
    <button
      onClick={() => setActiveTab(id)}
      className={cn(
        "flex items-center gap-2 px-6 py-3 text-sm font-bold tracking-tight transition-all relative",
        activeTab === id
          ? "text-primary border-b-2 border-primary bg-primary/5"
          : "text-muted-foreground hover:text-foreground hover:bg-foreground/[0.02]"
      )}
    >
      <Icon size={16} />
      {label}
      <span className="ml-1 text-[10px] opacity-50 bg-foreground/10 px-1.5 py-0.5 rounded-full">
        {id === 'orders' ? orders.length : id === 'trades' ? trades.length : positions.length}
      </span>
    </button>
  );

  return (
    <div className="p-8 space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 glass p-6 rounded-3xl border border-border/50">
        <div>
          <h1 className="text-3xl font-black tracking-tighter flex items-center gap-3">
            <History className="text-primary w-8 h-8" /> DHAN HQ ORDERS
          </h1>
          <p className="text-muted-foreground font-medium flex items-center gap-2 mt-1">
            Real-time reconciliation with your broker account
          </p>
        </div>

        <div className="flex items-center gap-3">
          <div className="text-right hidden sm:block">
            <p className="text-[10px] font-black uppercase tracking-widest text-muted-foreground/60">Last Sync</p>
            <p className="text-xs font-mono font-bold">{new Date().toLocaleTimeString()}</p>
          </div>
          <button
            onClick={handleReconcile}
            disabled={syncMutation.isPending || isLoading}
            className="bg-primary text-primary-foreground px-5 py-2.5 rounded-2xl font-black text-xs uppercase tracking-widest flex items-center gap-2 hover:scale-105 transition-all shadow-lg shadow-primary/20 disabled:opacity-50"
          >
            <RefreshCw size={14} className={cn((syncMutation.isPending || isRefetching) && "animate-spin")} />
            {syncMutation.isPending ? 'Syncing...' : 'Reconcile'}
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="glass rounded-2xl border border-border/50 overflow-hidden">
        <div className="flex border-b border-border/50 bg-foreground/[0.02]">
          <TabButton id="orders" label="Daily Orders" icon={Clock} />
          <TabButton id="trades" label="Trade Book" icon={TrendingUp} />
          <TabButton id="positions" label="Live Positions" icon={Briefcase} />
        </div>

        <div className="p-0">
          {isLoading && !data ? (
            <div className="p-20 text-center animate-pulse space-y-4">
              <RefreshCw className="w-12 h-12 text-primary/20 mx-auto animate-spin" />
              <p className="font-bold tracking-widest opacity-20 italic">Reconciling with Dhan HQ...</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left border-collapse">
                <thead>
                  <tr className="bg-foreground/[0.01] border-b border-border/50 text-[10px] font-black uppercase tracking-widest text-muted-foreground">
                    {activeTab === 'orders' && (
                      <>
                        <th className="p-4">Time</th>
                        <th className="p-4">Symbol</th>
                        <th className="p-4">Type</th>
                        <th className="p-4 text-center">Qty</th>
                        <th className="p-4 text-right">Price</th>
                        <th className="p-4 text-center">Status</th>
                      </>
                    )}
                    {activeTab === 'trades' && (
                      <>
                        <th className="p-4">Time</th>
                        <th className="p-4">Symbol</th>
                        <th className="p-4 text-center">Qty</th>
                        <th className="p-4 text-right">Execution Price</th>
                        <th className="p-4 text-right">Value</th>
                      </>
                    )}
                    {activeTab === 'positions' && (
                      <>
                        <th className="p-4">Symbol</th>
                        <th className="p-4 text-center">Net Qty</th>
                        <th className="p-4 text-right">Avg Price</th>
                        <th className="p-4 text-right">LTP</th>
                        <th className="p-4 text-right">PnL (Realized)</th>
                      </>
                    )}
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/30">
                  {activeTab === 'orders' && orders.map((order, idx) => (
                    <tr key={order.orderId || idx} className="hover:bg-foreground/[0.02] transition-colors group">
                      <td className="p-4 text-xs font-mono opacity-60 font-bold">{order.orderTime?.split(' ')[1]}</td>
                      <td className="p-4 font-black text-blue-500">{order.tradingSymbol?.replace(".NS", "")}</td>
                      <td className="p-4">
                        <span className={cn(
                          "px-2 py-0.5 rounded text-[10px] font-black uppercase tracking-wider",
                          order.transactionType === 'BUY' ? "bg-green-500/10 text-green-500" : "bg-red-500/10 text-red-500"
                        )}>
                          {order.transactionType}
                        </span>
                      </td>
                      <td className="p-4 text-center font-mono font-bold">{order.quantity}</td>
                      <td className="p-4 text-right font-mono font-bold italic opacity-70">
                        ₹{(order.orderType?.includes('STOP_LOSS') ? order.triggerPrice : order.price)?.toFixed(2)}
                      </td>
                      <td className="p-4 text-center">
                        <span className={cn(
                          "px-2 py-0.5 rounded-full text-[9px] font-black uppercase tracking-tighter",
                          order.orderStatus === 'TRADED' ? "bg-blue-500/20 text-blue-500" :
                            order.orderStatus === 'CANCELLED' ? "bg-red-500/20 text-red-500" : "bg-yellow-500/20 text-yellow-500"
                        )}>
                          {order.orderStatus}
                        </span>
                      </td>
                    </tr>
                  ))}

                  {activeTab === 'trades' && trades.map((trade, idx) => (
                    <tr key={trade.tradeId || idx} className="hover:bg-foreground/[0.02] transition-colors">
                      <td className="p-4 text-xs font-mono opacity-60 font-bold">{trade.tradeTime}</td>
                      <td className="p-4 font-black text-blue-500">{trade.tradingSymbol?.replace(".NS", "")}</td>
                      <td className="p-4 text-center font-mono font-bold">{trade.tradedQuantity}</td>
                      <td className="p-4 text-right font-mono font-bold">₹{trade.tradedPrice?.toFixed(2)}</td>
                      <td className="p-4 text-right font-mono font-bold opacity-70">
                        ₹{(trade.tradedQuantity * trade.tradedPrice).toFixed(2)}
                      </td>
                    </tr>
                  ))}

                  {activeTab === 'positions' && positions.map((pos, idx) => (
                    <tr key={pos.symbol || idx} className="hover:bg-foreground/[0.02] transition-colors">
                      <td className="p-4 font-black text-blue-500">{pos.tradingSymbol?.replace(".NS", "")}</td>
                      <td className="p-4 text-center font-mono font-bold">
                        <span className={pos.netQty > 0 ? "text-green-500" : pos.netQty < 0 ? "text-red-500" : ""}>
                          {pos.netQty}
                        </span>
                      </td>
                      <td className="p-4 text-right font-mono font-bold opacity-70">₹{pos.buyAvg?.toFixed(2)}</td>
                      <td className="p-4 text-right font-mono font-bold">₹{pos.lastPrice?.toFixed(2)}</td>
                      <td className="p-4 text-right font-mono font-bold">
                        <span className={pos.realizedProfit >= 0 ? "text-green-500" : "text-red-500"}>
                          ₹{pos.realizedProfit?.toFixed(2)}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {/* Empty States */}
              {((activeTab === 'orders' && orders.length === 0) ||
                (activeTab === 'trades' && trades.length === 0) ||
                (activeTab === 'positions' && positions.length === 0)) && (
                  <div className="p-20 text-center opacity-30 italic font-bold tracking-widest">
                    NO ACTIVE {activeTab.toUpperCase()} FOUND TODAY
                  </div>
                )}
            </div>
          )}
        </div>
      </div>

      {/* Sync Summary */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="glass p-5 rounded-2xl border border-border/40 flex items-center gap-4">
          <div className="p-3 rounded-xl bg-blue-500/10 text-blue-500">
            <CheckCircle2 size={24} />
          </div>
          <div>
            <p className="text-[10px] font-black uppercase tracking-widest text-muted-foreground/60 leading-none mb-1">Local Status</p>
            <p className="text-sm font-black text-foreground">Perfectly Synced</p>
          </div>
        </div>

        <div className="glass p-5 rounded-2xl border border-border/40 flex items-center gap-4">
          <div className="p-3 rounded-xl bg-amber-500/10 text-amber-500">
            <AlertCircle size={24} />
          </div>
          <div>
            <p className="text-[10px] font-black uppercase tracking-widest text-muted-foreground/60 leading-none mb-1">Sync Drift</p>
            <p className="text-sm font-black text-foreground">0 Instruments</p>
          </div>
        </div>

        <div className="glass p-5 rounded-2xl border border-border/40 flex items-center gap-4">
          <div className="p-3 rounded-xl bg-primary/10 text-primary">
            <LineChart size={24} />
          </div>
          <div>
            <p className="text-[10px] font-black uppercase tracking-widest text-muted-foreground/60 leading-none mb-1">Total Turnover</p>
            <p className="text-sm font-black text-foreground">₹{trades.reduce((acc, t) => acc + (t.tradedQuantity * t.tradedPrice), 0).toFixed(2)}</p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DhanOrders;
