using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddPeerCount : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "peer_count",
                table: "torrents",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<DateTime>(
                name: "peer_updated_at",
                table: "torrents",
                type: "timestamp with time zone",
                nullable: false,
                defaultValue: new DateTime(1, 1, 1, 0, 0, 0, 0, DateTimeKind.Unspecified));

            migrationBuilder.CreateIndex(
                name: "idx_torrents_peer_count",
                table: "torrents",
                column: "peer_count");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "idx_torrents_peer_count",
                table: "torrents");

            migrationBuilder.DropColumn(
                name: "peer_count",
                table: "torrents");

            migrationBuilder.DropColumn(
                name: "peer_updated_at",
                table: "torrents");
        }
    }
}
