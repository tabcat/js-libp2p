/**
 * @packageDocumentation
 *
 * A [libp2p transport](https://docs.libp2p.io/concepts/transports/overview/) based on [WebTransport](https://www.w3.org/TR/webtransport/).
 *
 * >
 * > ⚠️ **Note**
 * >
 * > This WebTransport implementation currently only allows dialing to other nodes. It does not yet allow listening for incoming dials. This feature requires QUIC support to land in Node JS first.
 * >
 * > QUIC support in Node JS is actively being worked on. You can keep an eye on the progress by watching the [related issues on the Node JS issue tracker](https://github.com/nodejs/node/labels/quic)
 * >
 *
 * @example
 *
 * ```TypeScript
 * import { createLibp2p } from 'libp2p'
 * import { webTransport } from '@libp2p/webtransport'
 * import { noise } from '@chainsafe/libp2p-noise'
 *
 * const node = await createLibp2p({
 *   transports: [
 *     webTransport()
 *   ],
 *   connectionEncryption: [
 *     noise()
 *   ]
 * })
 * ```
 */

import { noise } from '@chainsafe/libp2p-noise'
import { AbortError, CodeError, transportSymbol } from '@libp2p/interface'
import { WebTransport as WebTransportMatcher } from '@multiformats/multiaddr-matcher'
import { raceSignal } from 'race-signal'
import createListener from './listener.js'
import { webtransportMuxer } from './muxer.js'
import { inertDuplex } from './utils/inert-duplex.js'
import { isSubset } from './utils/is-subset.js'
import { parseMultiaddr } from './utils/parse-multiaddr.js'
import WebTransport from './webtransport.js'
import type { Transport, CreateListenerOptions, DialOptions, Listener, ComponentLogger, Logger, Connection, MultiaddrConnection, CounterGroup, Metrics, PeerId } from '@libp2p/interface'
import type { Multiaddr } from '@multiformats/multiaddr'
import type { Source } from 'it-stream-types'
import type { MultihashDigest } from 'multiformats/hashes/interface'
import type { Uint8ArrayList } from 'uint8arraylist'

/**
 * PEM format server certificate and private key
 */
export interface WebTransportCertificate {
  privateKey: string
  pem: string
  hash: MultihashDigest<number>
  secret: string
}

interface WebTransportSessionCleanup {
  (metric: string): void
}

export interface WebTransportInit {
  maxInboundStreams?: number
  certificates?: WebTransportCertificate[]
}

export interface WebTransportComponents {
  peerId: PeerId
  metrics?: Metrics
  logger: ComponentLogger
}

export interface WebTransportMetrics {
  dialerEvents: CounterGroup
}

class WebTransportTransport implements Transport {
  private readonly log: Logger
  private readonly components: WebTransportComponents
  private readonly config: Required<WebTransportInit>
  private readonly metrics?: WebTransportMetrics

  constructor (components: WebTransportComponents, init: WebTransportInit = {}) {
    this.log = components.logger.forComponent('libp2p:webtransport')
    this.components = components
    this.config = {
      ...init,
      maxInboundStreams: init.maxInboundStreams ?? 1000,
      certificates: init.certificates ?? []
    }

    if (components.metrics != null) {
      this.metrics = {
        dialerEvents: components.metrics.registerCounterGroup('libp2p_webtransport_dialer_events_total', {
          label: 'event',
          help: 'Total count of WebTransport dialer events by type'
        })
      }
    }
  }

  readonly [Symbol.toStringTag] = '@libp2p/webtransport'

  readonly [transportSymbol] = true

  async dial (ma: Multiaddr, options: DialOptions): Promise<Connection> {
    if (options?.signal?.aborted === true) {
      throw new AbortError()
    }

    this.log('dialing %s', ma)
    const localPeer = this.components.peerId
    if (localPeer === undefined) {
      throw new CodeError('Need a local peerid', 'ERR_INVALID_PARAMETERS')
    }

    options = options ?? {}

    const { url, certhashes, remotePeer } = parseMultiaddr(ma)
    let abortListener: (() => void) | undefined
    let maConn: MultiaddrConnection | undefined
    let cleanUpWTSession: WebTransportSessionCleanup = () => {}
    let closed = false
    let ready = false
    let authenticated = false

    try {
      this.metrics?.dialerEvents.increment({ pending: true })

      const wt = new WebTransport(`${url}/.well-known/libp2p-webtransport?type=noise`, {
        serverCertificateHashes: certhashes.map(certhash => ({
          algorithm: 'sha-256',
          value: certhash.digest
        }))
      })

      cleanUpWTSession = (metric: string) => {
        if (closed) {
          // already closed session
          return
        }

        try {
          this.metrics?.dialerEvents.increment({ [metric]: true })
          wt.close()
        } catch (err) {
          this.log.error('error closing wt session', err)
        } finally {
          // This is how we specify the connection is closed and shouldn't be used.
          if (maConn != null) {
            maConn.timeline.close = Date.now()
          }

          closed = true
        }
      }

      // if the dial is aborted before we are ready, close the WebTransport session
      abortListener = () => {
        if (ready) {
          cleanUpWTSession('noise_timeout')
        } else {
          cleanUpWTSession('ready_timeout')
        }
      }
      options.signal?.addEventListener('abort', abortListener, {
        once: true
      })

      this.log('wait for session to be ready')
      await Promise.race([
        wt.closed,
        wt.ready
      ])
      this.log('session became ready')

      ready = true
      this.metrics?.dialerEvents.increment({ ready: true })

      // this promise resolves/throws when the session is closed
      wt.closed.catch((err: Error) => {
        this.log.error('error on remote wt session close', err)
      })
        .finally(() => {
          cleanUpWTSession('remote_close')
        })

      authenticated = await raceSignal(this.authenticateWebTransport(wt, localPeer, remotePeer, certhashes), options.signal)

      if (!authenticated) {
        throw new CodeError('Failed to authenticate webtransport', 'ERR_AUTHENTICATION_FAILED')
      }

      this.metrics?.dialerEvents.increment({ open: true })

      maConn = {
        close: async () => {
          this.log('closing webtransport')
          cleanUpWTSession('close')
        },
        abort: (err: Error) => {
          this.log('aborting webtransport due to passed err', err)
          cleanUpWTSession('abort')
        },
        remoteAddr: ma,
        timeline: {
          open: Date.now()
        },
        log: this.components.logger.forComponent('libp2p:webtransport:maconn'),
        // This connection is never used directly since webtransport supports native streams.
        ...inertDuplex()
      }

      return await options.upgrader.upgradeOutbound(maConn, {
        skipEncryption: true,
        muxerFactory: webtransportMuxer(wt, wt.incomingBidirectionalStreams.getReader(), this.components.logger, this.config),
        skipProtection: true
      })
    } catch (err: any) {
      this.log.error('caught wt session err', err)

      if (authenticated) {
        cleanUpWTSession('upgrade_error')
      } else if (ready) {
        cleanUpWTSession('noise_error')
      } else {
        cleanUpWTSession('ready_error')
      }

      throw err
    } finally {
      if (abortListener != null) {
        options.signal?.removeEventListener('abort', abortListener)
      }
    }
  }

  async authenticateWebTransport (wt: WebTransport, localPeer: PeerId, remotePeer?: PeerId, certhashes: Array<MultihashDigest<number>> = [], signal?: AbortSignal): Promise<boolean> {
    if (signal?.aborted === true) {
      throw new AbortError()
    }

    const stream = await wt.createBidirectionalStream()
    const writer = stream.writable.getWriter()
    const reader = stream.readable.getReader()

    const duplex = {
      source: (async function * () {
        while (true) {
          const val = await reader.read()

          if (val.value != null) {
            yield val.value
          }

          if (val.done) {
            break
          }
        }
      })(),
      sink: async (source: Source<Uint8Array | Uint8ArrayList>) => {
        for await (const chunk of source) {
          await raceSignal(writer.ready, signal)

          const buf = chunk instanceof Uint8Array ? chunk : chunk.subarray()

          writer.write(buf).catch(err => {
            this.log.error('could not write chunk during authentication of WebTransport stream', err)
          })
        }
      }
    }

    const n = noise()(this.components)

    const { remoteExtensions } = await n.secureOutbound(localPeer, duplex, remotePeer)

    // We're done with this authentication stream
    writer.close().catch((err: Error) => {
      this.log.error(`Failed to close authentication stream writer: ${err.message}`)
    })

    reader.cancel().catch((err: Error) => {
      this.log.error(`Failed to close authentication stream reader: ${err.message}`)
    })

    // Verify the certhashes we used when dialing are a subset of the certhashes relayed by the remote peer
    if (!isSubset(remoteExtensions?.webtransportCerthashes ?? [], certhashes.map(ch => ch.bytes))) {
      throw new Error("Our certhashes are not a subset of the remote's reported certhashes")
    }

    return true
  }

  createListener (options: CreateListenerOptions): Listener {
    return createListener(this.components, {
      ...options,
      certificates: this.config.certificates,
      maxInboundStreams: this.config.maxInboundStreams
    })
  }

  /**
   * Filter check for all Multiaddrs that this transport can listen on
   */
  listenFilter (): Multiaddr[] {
    return []
  }

  /**
   * Filter check for all Multiaddrs that this transport can dial
   */
  dialFilter (multiaddrs: Multiaddr[]): Multiaddr[] {
    return multiaddrs.filter(ma => {
      if (!WebTransportMatcher.exactMatch(ma)) {
        return false
      }

      const { url, certhashes } = parseMultiaddr(ma)

      return url != null && certhashes.length > 0
    })
  }
}

export function webTransport (init: WebTransportInit = {}): (components: WebTransportComponents) => Transport {
  return (components: WebTransportComponents) => new WebTransportTransport(components, init)
}
