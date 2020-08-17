defmodule OffBroadwayBeanstalkd.MixProject do
  use Mix.Project

  def project do
    [
      app: :off_broadway_beanstalkd,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      description: description(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:broadway, "~> 0.6.0"},
      {:beanstix, "~> 0.1.0"},
      {:credo, "~> 1.4", only: :dev, runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      lint: ["format --check-formatted", "credo --strict"]
    ]
  end

  defp description() do
    "A beanstalkd connector for Broadway."
  end

  defp package() do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/fitronics/off_broadway_beanstalkd"}
    ]
  end
end
