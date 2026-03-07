import { useState } from 'react';
import { Play, Pause, Power, Activity } from 'lucide-react';
import { cn } from '../lib/utils';
import axios from 'axios';

const ServiceStatusCard = ({ serviceName, status, jobId, lastRun }) => {
  const [currentStatus, setCurrentStatus] = useState(status || 'STOPPED');
  const [loading, setLoading] = useState(false);

  const toggleService = async () => {
    setLoading(true);
    const action = currentStatus === 'RUNNING' ? 'stop' : 'start';
    try {
      await axios.post(`http://localhost:8000/system/jobs/${jobId}/${action}`);
      setCurrentStatus(action === 'start' ? 'RUNNING' : 'STOPPED');
    } catch (error) {
      console.error("Failed to toggle service", error);
    } finally {
      setLoading(false);
    }
  };

  const isRunning = currentStatus === 'RUNNING';

  return (
    <div className="glass group card-hover p-4 rounded-xl border border-border relative overflow-hidden">
      {/* Dynamic Glow Background */}
      <div className={cn(
        "absolute -right-4 -top-4 w-16 h-16 blur-2xl transition-opacity duration-500",
        isRunning ? "bg-green-500/10 opacity-100" : "bg-red-500/10 opacity-100"
      )} />

      <div className="flex items-center justify-between relative z-10">
        <div className="flex flex-col space-y-1">
          <div className="flex items-center gap-2">
            <div className={cn(
              "w-2 h-2 rounded-full",
              isRunning ? "bg-green-500 shadow-[0_0_8px_rgba(34,197,94,0.6)] animate-pulse" : "bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)]"
            )} />
            <h3 className="font-bold text-sm tracking-tight text-foreground uppercase">{serviceName}</h3>
          </div>
          <p className="text-[10px] font-mono text-muted-foreground uppercase flex items-center gap-1">
            <Activity className="w-3 h-3" />
            Last Run: {lastRun ? new Date(lastRun).toLocaleTimeString() : 'INIT'}
          </p>
        </div>

        <button
          onClick={toggleService}
          disabled={loading}
          className={cn(
            "p-2 rounded-lg transition-all duration-300",
            isRunning
              ? "bg-red-500/10 text-red-400 hover:bg-red-500 hover:text-white"
              : "bg-green-500/10 text-green-400 hover:bg-green-500 hover:text-white"
          )}
        >
          {loading ? (
            <div className="w-5 h-5 border-2 border-current border-t-transparent rounded-full animate-spin" />
          ) : (
            isRunning ? <Pause size={18} fill="currentColor" /> : <Play size={18} fill="currentColor" />
          )}
        </button>
      </div>
    </div>
  );
};

export default ServiceStatusCard;
