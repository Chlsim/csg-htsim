// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        
#include "randomqueue.h"
#include <math.h>
#include <iostream>

double RandomQueue::_static_plr = 0.0;

RandomQueue::RandomQueue(linkspeed_bps bitrate, mem_b maxsize,
                         EventList& eventlist, QueueLogger* logger, mem_b drop)
    : Queue(bitrate,maxsize,eventlist,logger),
      _drop(drop),
      _buffer_drops(0)
{
    //cout << "RandomQueue::_maxsize " << _maxsize << endl;
    _drop_th = _maxsize - _drop;
    _plr = _static_plr;
}

void RandomQueue::set_packet_loss_rate(double l){
    _plr = l;
}

void
RandomQueue::receivePacket(Packet& pkt) 
{
    double drop_prob = 0;
    int crt = _queuesize + pkt.size();

    // NDP's retransmit logic in this build relies entirely on the explicit
    // loss notification from trimming (header-only forwarding) -- RTO-based
    // recovery is compiled out (see rtx_timer_hook() in ndp.cpp, gated on
    // RESEND_ON_TIMEOUT, which is never defined). A silent full-packet drop
    // here previously stalled flows forever: no NACK was ever sent, so
    // nothing ever triggered a retransmit. Trim instead of dropping outright
    // -- same behavior CompositeQueue uses -- so NDP's normal NACK/fast-
    // retransmit path still fires for -plr-induced loss. Only trims packets
    // that still carry a payload; an already-header-only control packet
    // (ACK/NACK/PULL, or one trimmed upstream) is left alone.
    if (_plr > 0.0 && !pkt.header_only() && drand() < _plr){
        pkt.strip_payload();
        pkt.flow().logTraffic(pkt,*this,TrafficLogger::PKT_TRIM);
        if (_logger) _logger->logQueue(*this, QueueLogger::PKT_TRIM, pkt);
        crt = _queuesize + pkt.size(); // re-check occupancy with the now-smaller trimmed size
    }

    if (crt > _drop_th)
        drop_prob = 0.1;
  
    //  cout << "Drop Prob "<<drop_prob<< " queue size "<< _queuesize/1000 << " queue id " << id << endl;

    if (crt > _maxsize || drand() < drop_prob) {
        /* drop the packet */
        if (_logger) _logger->logQueue(*this, QueueLogger::PKT_DROP, pkt);
        pkt.flow().logTraffic(pkt, *this, TrafficLogger::PKT_DROP);
        //cout << "Buffer Drop" << endl;
        if (crt > _maxsize){
            _buffer_drops ++;
        }
        pkt.free();

        //cout << "Drop "<<drop_prob<< " queue size "<< _queuesize/1000 << " queue id " << _name << endl;

        return;
    }

    /* enqueue the packet */
    pkt.flow().logTraffic(pkt, *this, TrafficLogger::PKT_ARRIVE);
    bool queueWasEmpty = _enqueued.empty();
    Packet* pkt_p = &pkt;
    _enqueued.push(pkt_p);
    _queuesize += pkt.size();

    if (_logger) 
        _logger->logQueue(*this, QueueLogger::PKT_ENQUEUE, pkt);

    if (queueWasEmpty) {
        /* schedule the dequeue event */
        assert(_enqueued.size()==1);
        beginService();
    }
}
