#!/usr/bin/perl

use warnings;
use strict;

use DBIx::Class::Schema::Loader;
use DBIx::Class::_Util 'sigwarn_silencer';
use File::Basename 'dirname';
use SQL::Translator;

{
  package GraphedSchema;
  use base 'DBIx::Class::Schema::Loader';

  __PACKAGE__->loader_options (
    naming => 'v8',
    db_schema => 'spd',
    exclude => qr/^(?: debug  | metrics )/x,
  );
}

$SIG{__WARN__} = sigwarn_silencer(qr/collides with an inherited method/);

{
  no warnings 'redefine';
  *SQL::Translator::Schema::add_view = sub {
    my $s = shift;
    my %args = @_;
    my $t = $s->add_table(%args);
    $t->add_field(
      name => $_,
      size => 0,
      is_auto_increment => 0,
      is_foreign_key => 0,
      is_nullable => 0,
    ) for @{$args{fields}};
    return $t;
  };
}

my $schema = GraphedSchema->connect('dbi:Pg:service=spd');
$schema->storage->ensure_connected;
delete $schema->source('PublishedDeal')->{_relationships}{$_} for qw( proposal invalidated_deal ); # bugs, bugs everywhere :(

use Devel::Dwarn;

my $views = [qw( clients_datacap_available known_fildag_deals_ranked known_deals_ranked known_missized_deals )];

# this entire block exists solely to add all kinds of artificial "relationships"
# so that the diagram kinda-sorta makes sense
{
  # Ddie [ sort $schema->sources ];
  my $meta_rels = {
    client_id => [ Client => "client_id " ],
    deal_id => [ PublishedDeal => "deal_id" ],
    org_id => [ Provider => "provider_meta" ],
    city_id => [ Provider => "provider_meta" ],
    country_id => [ Provider => "provider_meta" ],
    continent_id => [ Provider => "provider_meta" ],
  };

  for (qw( MvDealsPrefilteredForRepcount ClientsDatacapAvailable KnownFildagDealRanked )) {
    $schema->source($_)->add_relationship( prop => "GraphedSchema::Result::PublishedDeal", { "foreign.piece_id" => "self.piece_id"} );
    $schema->source($_)->add_relationship( deal => "GraphedSchema::Result::Proposal", { "foreign.piece_id" => "self.piece_id"} );
  }

  $schema->source("MvPiecesAvailability")->add_relationship( prop => "GraphedSchema::Result::PublishedDeal", { "foreign.piece_id" => "self.piece_id"} );

  for my $srcname ( grep { $_ =~ /^Mv/ } $schema->sources ) {
    my $src = $schema->source($srcname);
    push @$views, $src->name;

    my $ci = $src->columns_info;
    delete $_->{is_nullable} for values %$ci;

    my $loctype;

    if( ( $loctype ) = $srcname =~ /MvOverreplicated(.+)/ ) {
      if( $loctype eq 'Total' ) {
        $src->add_relationship( $loctype => "GraphedSchema::Result::MvReplicasContinent", { "foreign.piece_id" => "self.piece_id"} );
      }
      else {
        my $col = lc($loctype) . "_id";
        $src->add_relationship( $loctype => "GraphedSchema::Result::MvReplicas$loctype", { "foreign.$col" => "self.$col"} );
      }
    }
    elsif ( ( $loctype ) = $srcname =~ /MvReplicas(.+)/ ) {
      $src->add_relationship( piece => "GraphedSchema::Result::MvDealsPrefilteredForRepcount", { "foreign.piece_id" => "self.piece_id"} );
    }
    else {
      $ci->{$_} and $src->add_relationship( $_ => "GraphedSchema::Result::$meta_rels->{$_}[0]", { "foreign.$meta_rels->{$_}[1]" => "self.$_"} )
        for ( keys %$meta_rels );
    }
  }
}

my $trans = SQL::Translator->new(
    parser        => 'SQL::Translator::Parser::DBIx::Class',
    parser_args   => { dbic_schema => $schema },
    producer      => 'GraphViz',
    producer_args => {
        width => 0,
        height => 0,
        output_type      => 'svg',
        out_file         => dirname(__FILE__) . '/pg_schema_diagram.svg',
        show_constraints => 1,
        show_datatypes   => 1,
        show_indexes     => 0, # this doesn't actually work on the loader side
        show_sizes       => 1,
        friendly_ints    => 1,
        cluster => [
           { name => "views", tables => $views },
        ],
    },
) or die SQL::Translator->error;
$trans->translate or die $trans->error;
