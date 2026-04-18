import { defaultLogger } from '@libp2p/logger'
import { expect } from 'aegir/chai'
import Sinon from 'sinon'
import { AbstractMessageStream } from '../src/abstract-message-stream.ts'
import type { MessageStreamInit, SendResult } from '../src/abstract-message-stream.ts'
import type { AbortOptions } from '@libp2p/interface'
import type { Uint8ArrayList } from 'uint8arraylist'

class TestStream extends AbstractMessageStream {
  public sendDataStub: Sinon.SinonStub<[Uint8ArrayList], SendResult>

  constructor (init: MessageStreamInit) {
    super(init)
    this.sendDataStub = Sinon.stub<[Uint8ArrayList], SendResult>()
  }

  sendData (data: Uint8ArrayList): SendResult {
    return this.sendDataStub(data)
  }

  sendReset (): void {}
  sendPause (): void {}
  sendResume (): void {}

  async close (_options?: AbortOptions): Promise<void> {}

  triggerDrain (): void {
    this.safeDispatchEvent('drain')
  }
}

describe('abstract-message-stream', () => {
  describe('drain after transport close race', () => {
    it('does not call sendData after the transport closes between drain and deferred processSendQueue', async () => {
      const stream = new TestStream({
        log: defaultLogger().forComponent('libp2p:test:abstract-message-stream')
      })

      // record the writeStatus at every sendData invocation so we can assert
      // that sendData is never called once the transport has been closed
      const writeStatusesAtSend: string[] = []
      stream.sendDataStub.callsFake((data) => {
        writeStatusesAtSend.push(stream.writeStatus)
        // first call: underlying resource is full, apply backpressure
        if (writeStatusesAtSend.length === 1) {
          return { sentBytes: 0, canSendMore: false }
        }
        return { sentBytes: data.byteLength, canSendMore: true }
      })

      stream.send(Uint8Array.from([1, 2, 3]))

      expect(stream.writableNeedsDrain).to.equal(true)
      expect(writeStatusesAtSend).to.deep.equal(['writable'])

      // the underlying resource drains, but in the same tick the transport
      // closes (e.g. RTCDataChannel close event fires). The drain listener
      // may defer processSendQueue via a microtask, in which case the close
      // handler runs first and sendData sees writeStatus === 'closed'.
      stream.triggerDrain()
      stream.onTransportClosed()

      expect(stream.writeStatus).to.equal('closed')

      // let any scheduled microtasks run
      await Promise.resolve()
      await Promise.resolve()

      // sendData must never have been called while writeStatus was 'closed'
      expect(writeStatusesAtSend).to.not.include('closed',
        'sendData was called while the transport was closed')
    })
  })
})
