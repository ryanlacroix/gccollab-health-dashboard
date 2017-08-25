FROM ruby:2.2.1
RUN apt-get update
RUN apt-get install -y net-tools
RUN apt-get upgrade -y

ENV APP_HOME /app
ENV HOME /root
RUN mkdir $APP_HOME
WORKDIR $APP_HOME
COPY Gemfile* $APP_HOME/
RUN apt-get install -y libblas-dev liblapack-dev
RUN apt-get install -y nodejs
RUN apt-get install -y python3.4
RUN apt-get install -y python3-pip
RUN bundle install
RUN pip3 install numpy pandas

COPY . $APP_HOME

ENV PORT 3030
EXPOSE 3030
CMD ["smashing", "start"]