# ibm-mq-streamsets

Custom stages based on streamsets datacollector standard jms-lib, but using the ibm client jms libraries

##Origin/Consumer
- Same as jms consumer, with additional config options that allow host/port/channel to be configured in streamsets rather than bindings file

##Target/Producer
- Same as jms producer, with additional config options that allow host/port/channel to be configured in streamsets rather than bindings file
- A replyTo queue can be optionally configured and will be applied to the outgoing message

##Processor
- No equivalent in standard jms lib, but based on the producer
- A replyTo queue can be optionally configured and will be applied to the outgoing message
- The message id generated on sending is captured and returned in the output record (Header attribute "MessageId")

##Release
- copy the tar.gz created by maven (e.g. ibm-mq-streamsets-1.0-SNAPSHOT.tar.gz) to streamsets user lib directory (/opt/streamsets-datacollector-user-libs) then "tar xvfz" the file and restart streamsets datacollector

##Warning!
Faced issues with building standard streamset lib packages (not available in maven) so a few of the dependencies have been copied inside a lib.ibmmq package in this project - may cause problems with upgrades and need reworked