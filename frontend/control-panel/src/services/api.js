import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000';

export const api = axios.create({
  baseURL: API_BASE_URL,
});

export const getSystemJobs = async () => {
  const response = await api.get('/system/jobs');
  return response.data;
};

export const getDailyFocus = async () => {
  const response = await api.get('/dashboard/focus');
  return response.data;
};

export const getSignals = async () => {
  const response = await api.get('/dashboard/signals');
  return response.data;
};

export const getNearMissSignals = async () => {
  const response = await api.get('/dashboard/signals/nearmiss');
  return response.data;
};

export const promoteNearMiss = async (nmId) => {
  const response = await api.post(`/dashboard/signals/nearmiss/${nmId}/promote`);
  return response.data;
};

export const demoteSignal = async (sigId) => {
  const response = await api.post(`/dashboard/signals/${sigId}/demote`);
  return response.data;
};

export const getPositions = async () => {
  const response = await api.get('/dashboard/positions');
  return response.data;
};

export const toggleJob = async (jobId, action) => {
  const response = await api.post(`/system/jobs/${jobId}/${action}`);
  return response.data;
};

export const getJobConfig = async (jobId) => {
  const response = await api.get(`/system/jobs/${jobId}/config`);
  return response.data;
};

export const updateJobConfig = async (jobId, config) => {
  const response = await api.post(`/system/jobs/${jobId}/config`, config);
  return response.data;
};

export const executeSignal = async (sigId) => {
  const response = await api.post(`/dashboard/signals/${sigId}/execute`);
  return response.data;
};

export const executePosition = async (posId) => {
  const response = await api.post(`/dashboard/positions/${posId}/execute`);
  return response.data;
};

export const getOracleResults = async () => {
  const response = await api.get('/system/oracle/results');
  return response.data;
};

export const getDhanSync = async () => {
  const response = await api.get('/dashboard/dhan/sync');
  return response.data;
};

export const triggerOracleSync = async () => {
  const response = await api.post('/system/oracle/sync');
  return response.data;
};
export const getDailyOpportunities = async () => {
  const response = await api.get('/analysis/daily-opportunities');
  return response.data;
};

export const getBridgeStatus = async () => {
  const response = await api.get('/system/bridge/status');
  return response.data;
};
