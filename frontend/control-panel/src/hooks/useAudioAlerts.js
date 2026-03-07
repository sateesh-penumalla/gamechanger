import { useState, useRef, useEffect, useCallback } from 'react';

export const useAudioAlerts = () => {
  const [isMuted, setIsMuted] = useState(() => {
    const saved = localStorage.getItem('audio_muted');
    return saved === null ? false : saved === 'true';  // Default to UNMUTED (false)
  });
  const [hasInteracted, setHasInteracted] = useState(false);

  // Audio assets
  const sounds = {
    chime: "/sounds/chime.wav",
    radar: "/sounds/radar.wav",
    notify: "/sounds/notify.wav" // mixkit-software-interface-start-2574
  };

  const audioRefs = useRef({});

  useEffect(() => {
    localStorage.setItem('audio_muted', isMuted);
  }, [isMuted]);

  const playSound = useCallback((type) => {
    if (isMuted || !hasInteracted) return;

    const url = sounds[type] || sounds.notify;
    const audio = new Audio(url);
    audio.play().catch(err => {
      console.log("Audio play failed:", err);
    });
  }, [isMuted, hasInteracted]);

  const toggleMute = () => {
    setIsMuted(prev => !prev);
    // First click to unmute counts as interaction
    setHasInteracted(true);
  };

  const unlockAudio = () => {
    if (!hasInteracted) {
      setHasInteracted(true);
      console.log("Audio context unlocked");
    }
  };

  return { isMuted, toggleMute, playSound, unlockAudio, hasInteracted };
};
