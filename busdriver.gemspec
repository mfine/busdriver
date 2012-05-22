Gem::Specification.new do |s|
  s.name = "busdriver"
  s.email = "mark.fine@gmail.com"
  s.version = "0.2"
  s.description = "A highly available redis bus client for Ruby apps."
  s.summary = "HA redis bus client"
  s.authors = ["Mark Fine"]
  s.homepage = "http://github.com/mfine/busdriver"

  s.files = Dir["lib/**/*.rb"] + Dir["Gemfile*"]
  s.require_paths = ["lib"]
  s.add_dependency "hiredis"
  s.add_dependency "redis"
  s.add_dependency "press"
end
