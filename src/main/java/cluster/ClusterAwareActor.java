package cluster;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorSelection;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

class ClusterAwareActor extends AbstractLoggingActor {
    private final Cluster cluster = Cluster.get(context().system());
    private final FiniteDuration tickInterval = Duration.create(10, TimeUnit.SECONDS);
    private Cancellable ticker;

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("tick", s -> tick())
                .match(Message.Ping.class, this::ping)
                .match(Message.Pong.class, this::pong)
                .build();
    }

    private void tick() {
        Member me = cluster.selfMember();
        log().debug("Tick {}", me);

        cluster.state().getMembers().forEach(member -> {
            if (!me.equals(member) && member.status().equals(MemberStatus.up())) {
                tick(member);
            }
        });
    }

    private void tick(Member member) {
        String path = member.address().toString() + self().path().toStringWithoutAddress();
        ActorSelection actorSelection = context().actorSelection(path);
        Message.Ping ping = new Message.Ping();
        log().debug("{} -> {}", ping, actorSelection);
        actorSelection.tell(ping, self());
    }

    private void ping(Message.Ping ping) {
        log().debug("{} <- {}", ping, sender());
        sender().tell(Message.Pong.from(ping), self());
    }

    private void pong(Message.Pong pong) {
        log().debug("{} <- {}", pong, sender());
    }

    @Override
    public void preStart() {
        log().debug("Start");
        ticker = context().system().scheduler()
                .schedule(Duration.Zero(),
                        tickInterval,
                        self(),
                        "tick",
                        context().system().dispatcher(),
                        null);
    }

    @Override
    public void postStop() {
        ticker.cancel();
        log().debug("Stop");
    }

    static Props props() {
        return Props.create(ClusterAwareActor.class);
    }

    interface Message {
        class Ping implements Serializable {
            final long time;

            Ping() {
                time = System.nanoTime();
            }

            @Override
            public String toString() {
                return String.format("%s[%dus]", getClass().getSimpleName(), time);
            }
        }

        class Pong implements Serializable {
            final long pingTime;

            private Pong(long pingTime) {
                this.pingTime = pingTime;
            }

            static Pong from(Ping ping) {
                return new Pong(ping.time);
            }

            @Override
            public String toString() {
                final double elapsed = (System.nanoTime() - pingTime) / 1000000000.0;
                return String.format("%s[elapsed %.9fs, %dus]", getClass().getSimpleName(), elapsed, pingTime);
            }
        }
    }
}
