import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { getJobConfig, updateJobConfig } from '../../services/api';
import { Save, RefreshCw, Sliders, ToggleLeft } from 'lucide-react';

const GenericJobSettings = ({ jobId, title, description }) => {
  const queryClient = useQueryClient();
  const { data: config, isLoading } = useQuery({ queryKey: ['job-config', jobId], queryFn: () => getJobConfig(jobId) });

  const mutation = useMutation({
    mutationFn: (newConfig) => updateJobConfig(jobId, newConfig),
    onSuccess: () => {
      queryClient.invalidateQueries(['job-config', jobId]);
      toast.success(`${title} configuration saved successfully`);
    },
    onError: (error) => {
      toast.error(`Failed to save configuration: ${error.message}`);
    }
  });

  const handleSave = (e) => {
    e.preventDefault();
    const formData = new FormData(e.target);
    const updatedConfig = {};

    Object.keys(config).forEach(key => {
      const val = formData.get(key);
      if (typeof config[key] === 'boolean') {
        updatedConfig[key] = formData.has(key); // Checkboxes are 'on' if checked, missing if not
      } else if (typeof config[key] === 'number') {
        updatedConfig[key] = parseFloat(val);
      } else {
        updatedConfig[key] = val;
      }
    });

    mutation.mutate(updatedConfig);
  };

  const renderField = (key, value) => {
    if (typeof value === 'boolean') {
      return (
        <div key={key} className="flex items-center justify-between p-3 bg-accent/20 rounded-lg border border-border/40">
          <label className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">{key.replace(/_/g, ' ')}</label>
          <input type="checkbox" name={key} defaultChecked={value} className="accent-primary" />
        </div>
      );
    }
    return (
      <div key={key} className="space-y-1.5 p-3 bg-accent/20 rounded-lg border border-border/40">
        <label className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">{key.replace(/_/g, ' ')}</label>
        <input
          type={typeof value === 'number' ? 'number' : 'text'}
          step={typeof value === 'number' ? 'any' : undefined}
          name={key}
          defaultValue={value}
          className="w-full h-6 text-[11px] bg-background border border-border rounded px-2"
        />
      </div>
    );
  };

  if (isLoading) return <div className="p-10 text-[10px] font-bold tracking-widest opacity-50 animate-pulse">LOADING CONFIG...</div>;

  return (
    <form onSubmit={handleSave} className="space-y-6 animate-in fade-in zoom-in-95 duration-300">
      <div className="flex items-center justify-between border-b border-border pb-4">
        <div>
          <h1 className="text-lg font-black tracking-tight flex items-center gap-2 uppercase">
            {title}
          </h1>
          <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest">{description}</p>
        </div>
        <button
          type="submit"
          disabled={mutation.isPending}
          className="flex items-center gap-2 bg-primary text-white px-4 py-1.5 rounded-lg hover:bg-primary/90 disabled:opacity-50 transition-all font-black text-[10px] uppercase tracking-widest shadow-lg shadow-primary/20 active:scale-95"
        >
          <Save size={12} /> {mutation.isPending ? 'SAVING...' : 'SAVE'}
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {Object.keys(config || {}).length === 0 ? (
          <div className="col-span-full p-10 text-center text-muted-foreground opacity-50 italic">No configurable parameters.</div>
        ) : (
          Object.entries(config).map(([k, v]) => renderField(k, v))
        )}
      </div>
    </form>
  );
};

export default GenericJobSettings;
